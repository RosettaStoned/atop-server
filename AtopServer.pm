package AtopServer;

use strict;
use warnings;
use Log::Log4perl qw(:easy);
use AnyEvent;
use AnyEvent::HTTPD;
use JSON qw( to_json );
use DateTime;

sub new {

    my ( $class, %options ) = @_;

    my $self = {
        file => "/var/tmp/atop-server",
        json => "",
        fork => undef,
        child => undef,
        %options
    };

    if(!defined $$self{interval}) {
        $$self{interval} = 60;
    }

    bless $self, $class

}

sub start {
    my ( $self ) = @_;

    $$self{timer} = AnyEvent->timer(
        after => 0,
        interval => $$self{interval},
        cb => sub {
            if( defined $$self{fork} ) {
                DEBUG "atop already running";
                return 1
            }
            $self->atop_spawn();
        }
    );

    $self->httpd_spawn();
}

sub atop_spawn {
    my ( $self ) = @_;

    $$self{fork} = fork();

    if(!defined $$self{fork}) {
        LOGDIE "Waaaah, failed to fork!";
    }

    if($$self{fork}) {
        #parent
        
        $$self{child} = AnyEvent->child(
            pid => $$self{fork}, 
            cb => sub {
                $self->atop_parse();
                $$self{fork} = undef;
            }
        );

    } else {
        #child
        
        $$self{time_zone} = 
            DateTime::TimeZone->new( name => 'local' );
        $$self{dt} = 
            DateTime->now( time_zone => $$self{time_zone} );
        $$self{atop_file} = 
            "/var/log/atop/atop_" . $$self{dt}->ymd("");
        
        $$self{end_time} = $$self{dt}->hms();
        $$self{dt}->subtract( seconds => $$self{interval} );
        $$self{start_time} = $$self{dt}->hms();

        $$self{atop_cmd} = 
            "atop -P ALL -r $$self{atop_file} -b $$self{start_time} -e $$self{end_time} > $$self{file}";
        exec $$self{atop_cmd};

    }
}

sub httpd_spawn {
    my ( $self ) = @_;

    $$self{httpd} = 
        AnyEvent::HTTPD->new( port => 9402 );

    $$self{httpd}->reg_cb(
        '/' => sub {
            my ($httpd, $req) = @_;

            $req->respond({ content => 
                    ["text/json", $$self{json}]
                }
            );
        }
    );

}

sub atop_parse {
    my ( $self ) = @_;

    $$self{stats} = {};

    my $to_bytes = {
        b => sub ($){
            my ($value) = @_;
        },
        kb => sub ($){
            my ($value) = @_;
            $value <<= 10;
        },
        mb => sub ($){
            my ($value) = @_;
            $value <<= 20;
        },
        gb => sub ($){
            my ($value) = @_;
            $value <<= 30;
        },
        tb => sub ($){
            my ($value) = @_;
            $value <<= 40;
        }
    };

    my $lvm_mdd_dsk = sub ($@) {
        my ($label, $stat, @fields) = @_;

        my $dev = $fields[0];

        $$self{stats}{$stat}{devices}{$dev} = {
            name => {
                value => $fields[0]
            },
            num_ms_spent_io => {
                descr => "number of milliseconds spent for I/O",
                value => $fields[1],
                units => "ms"
            },
            num_issued_reads => {
                descr => "number of reads issued",
                value => $fields[2]
            },
            num_sect_transf_reads => {
                descr => "number of sectors transferred for reads",
                value => $fields[3]
            },
            num_issued_writes => {
                descr => "number of writes issued",
                value => $fields[4]
            },
            num_sect_transf_write => {
                descr => "number  of  sectors  transferred for write",
                value => $fields[5]
            },
            num_sect_transf_per_sec => {
                descr => "number of sectors transferred per seconds",
                value => $fields[1] > 0 
                ?   ($fields[3] + $fields[5]) / ($fields[1] / 1000)
                : 0
            }
        };

        $$self{stats}{$stat}{num_sect_transf_per_sec} //= {
            descr => "number of sectors transferred per seconds",
            value => 0
        };

        $$self{stats}{$stat}{num_sect_transf_per_sec}{value} +=
            $$self{stats}{$stat}{devices}{$dev}{num_sect_transf_per_sec};

    };

    my $fields_to_href = {
        cpu => sub ($@) {
            my ($label, $stat, @fields) = @_;

            my $cpu_href = {
                num_clock_ticks_per_sec => {
                    descr => "total  number of clock-ticks per second for this machine",
                    value => $fields[0],
                },
                ($label eq "CPU") ? 
                (
                    num_processors => {
                        descr => "number of processors",
                        value => $fields[1]
                    },
                ) : 
                (
                    processor_num => {
                        descr => "processor number",
                        value => $fields[1]
                    }
                ),
                system_mode => {
                    descr => "consumption for all CPU's in system mode (clock-ticks)",
                    value => $fields[2],
                    units => "clock-ticks"
                },
                user_mode => {
                    descr => "consumption for all CPU's in user mode (clock-ticks)",
                    value => $fields[3],
                    units => "clock-ticks"
                },
                user_mode_niced_procs => {
                    descr => "consumption for all CPU's in user mode for niced processes (clock-ticks)",
                    value => $fields[4],
                    units => "clock-ticks"
                },
                idle_mode => {
                    descr => "consumption for all CPU's in idle mode (clock-ticks)",
                    value => $fields[5],
                    units => "clock-ticks"
                },
                wait_mode => {
                    descr => "onsumption  for  all  CPU's  in wait  mode  (clock-ticks)",
                    value => $fields[6],
                    units => "clock-ticks"
                },
                irq_mode => {
                    descr => "consumption for all CPU's in irq mode (clock-ticks)",
                    value => $fields[7],
                    units => "clock-ticks"
                },
                softirq_mode => {
                    descr => "consumption for all CPU's in softirq mode (clock-ticks)",
                    value => $fields[8],
                    units => "clock-ticks"
                },
                steal_mode => {
                    descr => "consumption for all CPU's in steal mode (clock-ticks)",
                    value => $fields[9],
                    units => "clock-ticks"
                },
                guest_mode => {
                    descr => "consumption for all CPU's in guest mode (clock-ticks)",
                    value => $fields[9],
                    units => "clock-ticks"
                }
            };

            my $processor_num = $$cpu_href{processor_num}{value};

            if($label eq "CPU") {
                $$self{stats}{$stat} = $cpu_href;  
            } else {
                $$self{stats}{$stat}{processors}{$processor_num} = $cpu_href;
            }
        },
        cpl => sub ($@) {
            my ($label, $stat, @fields) = @_;

            $$self{stats}{$stat} = {
                num_processors => {
                    descr => "number of processors",
                    value => $fields[0],
                },
                load_avg_one_min => {
                    descr => "load average for last minute",
                    value => $fields[1],
                },
                load_avg_five_min => {
                    descr => "load average for last five minutes",
                    value => $fields[2],
                },
                load_avg_fifteen_min => {
                    descr => "load average for last fifteen minutes",
                    value => $fields[3],
                },
                num_context_switches => {
                    descr => "number of context-switches",
                    value => $fields[4],
                },
                num_device_int => {
                    descr => "number of device interrupts",
                    value => $fields[4],
                }
            };

        },
        mem => sub ($@) {
            my ($label, $stat, @fields) = @_;

            $$self{stats}{$stat} = {
                page_size => {
                    descr => "page size for this machine (in bytes)",
                    value => $fields[0],
                    units => "bytes",
                },
                mem_physical_size => {
                    descr => "size of physical memory (pages)",
                    value => $fields[1],
                    units => "pages"
                },
                mem_free_size => {
                    descr => "size of free memory (pages)",
                    value => $fields[2],
                    units => "pages"
                },
                page_cache_size => {
                    descr => "size of page cache (pages)",
                    value => $fields[3],
                    units => "pages"
                },
                buffer_cache_size => {
                    descr => "size of buffer cache (pages)",
                    value => $fields[4],
                    units => "pages" 
                },
                slab_size => {
                    descr => "size of slab (pages)",
                    value => $fields[5],
                    units => "pages"
                },
                num_dirty_pages_in_cache => {
                    descr => "number of dirty pages in cache",
                    value => $fields[6]
                }
            };

        },
        swp => sub ($@) {
            my ($label, $stat, @fields) = @_;

            $$self{stats}{$stat} = {
                page_size => {
                    descr => "page size for this machine (in bytes)",
                    value => $fields[0],
                    units => "bytes",
                },
                swap_size => {
                    descr => "size of swap (pages)",
                    value => $fields[1],
                    units => "pages"
                },
                swap_free_size => {
                    descr => "size of free swap (pages)",
                    value => $fields[2],
                    units => "pages"
                },
                commited_space_size => {
                    descr => "size of committed space (pages)",
                    value => $fields[4],
                    units => "pages" 
                },
                commited_space_limit => {
                    descr => "limit for committed space (pages)",
                    value => $fields[5],
                    units => "pages"
                }
            };

        },
        pag => sub ($@) {
            my ($label, $stat, @fields) = @_;

            $$self{stats}{$stat} = {
                page_size => {
                    descr => "page size for this machine (in bytes)",
                    value => $fields[0],
                    units => "bytes",
                },
                num_page_scans => {
                    descr => "number of page scans",
                    value => $fields[1]
                },
                num_allocstalls => {
                    descr => "number of allocstalls",
                    value => $fields[2]
                },
                num_swapins => {
                    descr => "number of swapins",
                    value => $fields[4]
                },
                num_swapouts => {
                    descr => "number of swapouts",
                    value => $fields[5]
                }
            };

        },
        lvm => $lvm_mdd_dsk,
        mdd => $lvm_mdd_dsk,
        dsk => $lvm_mdd_dsk,
        net => sub ($@) {
            my ($label, $stat, @fields) = @_;

            if($fields[0] eq "upper")
            {
                $$self{stats}{$stat} = {
                    tcp_num_received_packets => {
                        descr => "number of packets received by TCP",
                        value => $fields[1]
                    },
                    tcp_num_transmitted_packets => {
                        descr => "number of packets transmitted by TCP",
                        value => $fields[2]
                    },
                    udp_num_received_packets => {
                        descr => "number of packets received by UDP",
                        value => $fields[3]
                    },
                    udp_num_transmitted_packets => {
                        descr => "number of packets transmitted by UDP",
                        value => $fields[4]
                    },
                    num_packets_received_ip => {
                        descr => "number of packets received by IP",
                        value => $fields[5]
                    },
                    num_packets_transmitted_ip => {
                        descr => "number of packets transmitted by IP",
                        value => $fields[6]
                    },
                    num_packets_deliv_high_layer_ip => {
                        descr => "number of packets delivered to higher layers by IP",
                        value => $fields[7]
                    },
                    num_packets_forwarded_ip => {
                        descr => "number of packets forwarded by IP",
                        value => $fields[8]
                    }
                };

            }
            else
            {
                my $int = $fields[0];

                $$self{stats}{$stat}{interfaces}{$int} = {
                    name => {
                        descr => "name of the interface",
                        value => $fields[0]
                    },
                    num_received_packets => {
                        descr => "number of packets received by the interface",
                        value => $fields[1]
                    },
                    num_received_bytes => {
                        descr => "number of bytes received by the interface",
                        value => $fields[2],
                        units => "bytes"
                    },
                    num_transmitted_packets => {
                        descr => "number of packets transmitted by the interface",
                        value => $fields[3],
                    },
                    num_transmitted_bytes => {
                        descr => "number of bytes transmitted by the interface",
                        value => $fields[4], 
                        units => "bytes"
                    },
                    interface_speed => {
                        descr => "interface speed",
                        value => $fields[5]
                    },
                    duplex_mode => {
                        descr => "duplex mode (0=half, 1=full)",
                        value => $fields[6]
                    }
                };

            }

        },
        prg => sub ($@) {
            my ($label, $stat, @fields) = @_;

            my $pid = $fields[0];

            $$self{stats}{$stat}{pids}{$pid} = {
                pid => {
                    descr => "PID",
                    value => $fields[0]
                },
                name => {
                    descr => "process name",
                    value => $fields[1],
                },
                state => {
                    descr => "process state",
                    value => $fields[2]
                },
                real_uid => {
                    descr => "real uid",
                    value => $fields[3]
                },
                real_gid => {
                    descr => "real gid",
                    value => $fields[4]
                },
                tgid => {
                    descr => "TGID (same as PID)",
                    value => $fields[5]
                },
                num_threads => {
                    descr => "total number of threads",
                    value => $fields[6]
                },
                exit_code => {
                    descr => "exit code",
                    value => $fields[7]
                },
                start_time => {
                    descr => "start time (epoch)",
                    value => $fields[8]
                },
                cmdline => {
                    descr => "full command line (between brackets)",
                    value => $fields[9]
                },
                ppid => {
                    descr => "PPID",
                    value => $fields[10]
                },
                num_threads_r => {
                    descr => "number of threads in state 'running' (R)",
                    value => $fields[11]
                },
                num_threads_s => {
                    descr => "number of threads in state 'interruptible sleeping' (S)",
                    value => $fields[12]
                },
                num_threads_d => {
                    descr => "number of threads in state 'uninterruptible sleeping' (D)",
                    value => $fields[13]
                },
                effective_uid => {
                    descr => "effective uid",
                    value => $fields[14] 
                },
                effective_gid => {
                    descr => "effective gid",
                    value => $fields[15]
                },
                saved_uid => {
                    descr => "saved uid",
                    value => $fields[16]
                },
                saved_gid => {
                    descr => "saved gid",
                    value => $fields[17]
                }, 
                filesystem_uid => {
                    descr => "filesystem uid",
                    value => $fields[18]
                }, 
                filesystem_gid => {
                    descr => "filesystem gid",
                    value => $fields[19]
                },
                elapsed_time => {
                    descr => "elapsed time (hertz)",
                    value => $fields[20],
                    units => "hertz"
                }
            };

        },
        prc => sub ($@) {
            my ($label, $stat, @fields) = @_;
			
            my $pid = $fields[0];

            $$self{stats}{$stat}{pids}{$pid} = {
                pid => {
                    descr => "PID",
                    value => $fields[0]
                },
                name => {
                    descr => "process name",
                    value => $fields[1],
                },
                state => {
                    descr => "process state",
                    value => $fields[2]
                },
                num_clock_ticks_per_sec => {
                    descr => "total  number of clock-ticks per second for this machine",
                    value => $fields[3],
                },
                user_mode => {
                    descr => "CPU-consumption in user mode (clockticks)",
                    value => $fields[4],
                    units => "clock-ticks"
                },
                system_mode => {
                    descr => "CPU-consumption in system mode (clockticks)",
                    value => $fields[5],
                    units => "clock-ticks"
                },
                nice_value => {
                    descr => "nice value",
                    value => $fields[6]
                },
                priority => {
                    descr => "priority",
                    value => $fields[7]
                },
                realtime_priority => {
                    descr => "realtime priority",
                    value => $fields[8]
                },
                scheduling_policy => {
                    descr => "scheduling policy",
                    value => $fields[9]
                },
                current_cpu => {
                    descr => "current CPU",
                    value => $fields[10]
                },
                sleep_avg => {
                    descr => "sleep average",
                    value => $fields[11]
                },
                cpu_perc => {
                    descr => "CPU-consumption (percentage)",
                    value => 100 * ($fields[5] + $fields[4]) / ($fields[3] * $$self{interval})
                }
            };

        },
        prm => sub ($@) {
            my ($label, $stat, @fields) = @_;

            my $pid = $fields[0];

            $$self{stats}{$stat}{pids}{$pid} = {
                pid => {
                    descr => "PID",
                    value => $fields[0]
                },
                name => {
                    descr => "process name",
                    value => $fields[1],
                },
                state => {
                    descr => "process state",
                    value => $fields[2]
                },
                page_size => {
                    descr => "page size for this machine (in bytes)",
                    value => $fields[3],
                    units => "bytes",
                },
                mem_virtual => {
                    descr => "virtual memory size (bytes)",
                    value => $$to_bytes{kb}->($fields[4]),
                    units => "bytes"
                }, 
                mem_resident => {
                    descr => "resident memory  size  (bytes)",
                    value => $$to_bytes{kb}->($fields[5]),
                    units => "bytes"
                }, 
                mem_shared => {
                    descr => "shared  text  memory  size (bytes)",
                    value => $$to_bytes{kb}->($fields[6]),
                    units => "bytes"
                },
                mem_swap => {
                    descr => "swaped memory size (bytes)",
                    value => 0,
                    units => "bytes"
                },
                mem_virtual_growth => {
                    descr => "virtual memory growth (bytes)",
                    value => $$to_bytes{kb}->($fields[7]),
                    units => "bytes"
                }, 
                mem_resident_growth => {
                    descr => "resident memory growth (bytes)",
                    value => $$to_bytes{kb}->($fields[8]),
                    units => "bytes"
                }, 
                num_minor_page_faults => {
                    descr => "number of minor page faults",
                    value => $fields[9]
                }, 
                num_major_page_faults => {
                    descr => "number of major page faults",
                    value => $fields[10]
                },
                mem_perc => {
                    descr => "memory (percentage)",
                    value => 100 * $$to_bytes{kb}->($fields[5]) / 
                        ($$self{stats}{mem}{mem_physical_size} * $fields[3]),
                    units => "percentage"
                },
                swap_perc => {
                    descr => "swap (percentage)",
                    value => 0,
                    units => "percentage"
                },
            };

            my $fh;
            if(!open($fh, "<", "/proc/$pid/smaps")) {
                LOGWARN "Cannot open /proc/$pid/smaps: $!";
                return;
            }

            while(my $line = <$fh>) {
                if($line =~ m/^Swap:\s+(\d+)\s+(kb|mg|gb|tb)$/i) {
                    my $val = $1;
                    my $unit = $2;

                    $$self{stats}{$stat}{pids}{$pid}{mem_swap}{value} +=
                        $$to_bytes{lc($unit)}->($val);
                }
            }

            if(!close($fh)) {
                LOGWARN "Cannot close /proc/$pid/smaps: $!";
            }

            $$self{stats}{$stat}{pids}{$pid}{swap_perc}{value} = 
                100 * $$self{stats}{$stat}{pids}{$pid}{mem_swap}{value} /
                ($$self{stats}{swp}{swap_size} * $$self{stats}{swp}{page_size});

        },
        prd => sub ($@) {
            my ($label, $stat, @fields) = @_;

            my $pid = $fields[0];

            $$self{stats}{$stat}{pids}{$pid} = {
                pid => {
                    descr => "PID",
                    value => $fields[0]
                },
                name => {
                    descr => "process name",
                    value => $fields[1],
                },
                state => {
                    descr => "process state",
                    value => $fields[2]
                },
                kernel_patch_installed =>{
                    descr => "kernel-patch installed ('y' or 'n')",
                    value => $fields[3]
                },
                std_io_stats_used => {
                    descr => "standard io statistics used ('y' or 'n')",
                    value => $fields[4]
                },
                num_disk_reads => {
                    descr => "number of reads on disk",
                    value => $fields[5]
                },
                num_sectors_read => {
                    descr => "cumulative number of sectors read",
                    value => $fields[6]
                },
                num_disk_writes => {
                    descr => "number of writes on disk",
                    value => $fields[7]
                },
                num_sectors_written => {
                    descr => "cumulative number of sectors written",
                    value => $fields[8]
                },
                num_cancelled_sectors_written =>{
                    descr => "cancelled number of written sectors",
                    value => $fields[9]
                },
                num_io => {
                    descr => "number of io operations issued to disk",
                    value => $fields[6] + $fields[8]
                },
                io_perc => {
                    descr => "consumption of io (percentage)",
                    value => 100 * ($fields[6] + $fields[8]) / 
                        ($$self{stats}{dsk}{num_sect_transf_per_sec}{value} * $$self{interval})
                }
            };

        },
        prn => sub ($@) {
            my ($label, $stat, @fields) = @_;

            my $pid = $fields[0];

            $$self{stats}{$stat}{pids}{$pid} = {
                pid => {
                    descr => "PID",
                    value => $fields[0]
                },
                name => {
                    descr => "process name",
                    value => $fields[1],
                },
                state => {
                    descr => "process state",
                    value => $fields[2]
                },
                kernel_patch_installed =>{
                    descr => "kernel-patch installed ('y' or 'n')",
                    value => $fields[3]
                },
                tcp_num_transmitted_packets => {
                    descr => "number of TCP-packets transmitted",
                    value => $fields[4]
                },
                tcp_size_transmitted_packets => {
                    descr => "cumulative size of  TCP-packets  transmitted",
                    value => $fields[5]
                },
                tcp_num_received_packets => {
                    descr => "number  of TCP-packets  received",
                    value => $fields[6]
                },
                tcp_size_received_packets => {
                    descr => "cumulative size of TCP-packets received",
                    value => $fields[7]
                },
                udp_num_transmitted_packets => {
                    descr => "number of UDP-packets transmitted",
                    value => $fields[8]
                }, 
                udp_size_transmitted_packets => {
                    descr => "cumulative size of UDP-packets transmitted",
                    value => $fields[9]
                }, 
                udp_num_received_packets => {
                    descr => "number of UDP-packets received, cumulative",
                    value => $fields[10]
                },
                udp_size_reveived_packets => {
                    descr => "cumulative size of UDP-packets received",
                    value => $fields[11]
                },
                raw_num_transmitted_packets => {
                    descr => "number of raw packets transmitted",
                    value => $fields[12]
                }, 
                raw_num_received_packets => { 
                    descr => "number of raw packets received",
                    value => $fields[13]
                }
            };

        }
    };   



    open(my $fh, "<", $$self{file});
    while(my $line = <$fh>) {

        if($line =~ m/^reset|sep/i){
            next();
        }

        my @matches = ();
        while($line =~ s/(\(([^()]|(?R))*\))//) {

            if(defined($1)) {
                my $match;
                ($match = $1) =~ s/\(|\)|^\s+|\s+$//g;
                push(@matches, $match);
            }

        } 

        my @splited_line = split(/\s+/, $line);

        my ($label, 
            $host, 
            $epoch, 
            $date, 
            $time, 
            $interval, 
            @fields) = @splited_line;


        if(@matches) {
            my ($pname, $pcmdline) = @matches;
            my $pid = shift(@fields);

            if($label eq "PRG")
            {
                splice(@fields, 7, 0, $pcmdline);
            }

            @fields = ($pid, $pname, @fields);
        }

        my $stat = lc($label);
        if(!defined $$fields_to_href{$stat}) {
            LOGDIE "Unrecognise atop stat '$stat'";
        }
        $$fields_to_href{$stat}->($label, $stat, @fields);
    }
    close($fh);

    $$self{json} = to_json($$self{stats}, {
            pretty => 1    
        }
    );
}

1;
