#!/usr/bin/perl

#------------------------------------------------------------------------------
# Copyright (c) 2007-2009, Stanford University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Stanford University nor the names of its 
#       contributors may be used to endorse or promote products derived from 
#       this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#------------------------------------------------------------------------------ 

use strict;

if ($#ARGV + 1 != 1) {
    print "Usage: breakdown.pl input_file\n";
    exit;
}

my $filename = $ARGV[0];
open (INPUT_FILE, $filename) || die "Could not open file [$filename]";

my %init_time;
my %library_init_time;
my %map_work_time;
my %map_user_time;
my %map_combiner_time;
my %map_time;
my %reduce_work_time;
my %reduce_user_time;
my %reduce_time;
my %merge_work_time;
my %merge_time;
my %library_fin_time;
my %inter_library_time;
my %fin_time;

my $line;
my @fields;
my $exp_id = -1;
my $iteration;
while ($line = <INPUT_FILE>)
{
    chomp ($line);
    @fields = split (/:/, $line);
    
    if (@fields[0] eq "initialize")
    {
        $exp_id += 1;
        $iteration = -1;
        $init_time{$exp_id} = @fields[1];
    }
    elsif (@fields[0] eq "library init")
    {
        $iteration += 1;
        $library_init_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "map work time")
    {
        $map_work_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "map user time")
    {
        $map_user_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "map combiner time")
    {
        $map_combiner_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "map phase")
    {
        $map_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "reduce work time")
    {
        $reduce_work_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "reduce user time")
    {
        $reduce_user_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "reduce phase")
    {
        $reduce_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "merge work time")
    {
        $merge_work_time{$exp_id}{$iteration} += @fields[1];
    }
    elsif (@fields[0] eq "merge phase")
    {
        $merge_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "library finalize")
    {
        $library_fin_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "inter library")
    {
        $inter_library_time{$exp_id}{$iteration} = @fields[1];
    }
    elsif (@fields[0] eq "finalize")
    {
        $fin_time{$exp_id} = @fields[1];
    }
}

my $init_time;
my $library_init_time;
my $map_work_time;
my $map_user_time;
my $map_combiner_time;
my $map_time;
my $reduce_work_time;
my $reduce_user_time;
my $reduce_time;
my $merge_work_time;
my $merge_time;
my $library_fin_time;
my $inter_library_time;
my $fin_time;

my $total_exps = $exp_id;
my $it;
my @threads = ( 1, 2, 4, 8, 16, 32, 64, 128, 256 );
my $batch = 4;
my $thread;

for ($thread = 0; $thread < scalar(@threads); ++$thread) {
    $init_time = 0;
    $library_init_time = 0;
    $map_work_time = 0;
    $map_user_time = 0;
    $map_combiner_time = 0;
    $map_time = 0;
    $reduce_work_time = 0;
    $reduce_user_time = 0;
    $reduce_time = 0;
    $merge_work_time = 0;
    $merge_time = 0;
    $library_fin_time = 0;
    $inter_library_time = 0;
    $fin_time = 0;

    for ($exp_id = $thread * $batch; $exp_id < ($thread + 1) * $batch; 
         ++$exp_id) {
        $init_time += $init_time{$exp_id};
        for ($it = 0; $it < keys (%{$library_init_time{$exp_id}}); ++$it) {
            $library_init_time += $library_init_time{$exp_id}{$it};
            $map_work_time += $map_work_time{$exp_id}{$it};
            $map_user_time += $map_user_time{$exp_id}{$it};
            $map_combiner_time += $map_combiner_time{$exp_id}{$it};
            $map_time += $map_time{$exp_id}{$it};
            $reduce_work_time += $reduce_work_time{$exp_id}{$it};
            $reduce_user_time += $reduce_user_time{$exp_id}{$it};
            $reduce_time += $reduce_time{$exp_id}{$it};
            $merge_work_time += $merge_work_time{$exp_id}{$it};
            $merge_time += $merge_time{$exp_id}{$it};
            $library_fin_time += $library_fin_time{$exp_id}{$it};
        }
        for ($it = 0; $it < keys (%{$inter_library_time{$exp_id}}); ++$it) {
            $inter_library_time += $inter_library_time{$exp_id}{$it};
        }
        $fin_time += $fin_time{$exp_id};
    }

    print ("\n");
    print ("@threads[$thread]\n");
    print ("---------------\n");

    print ($init_time / $batch . "\n");
    print ($library_init_time / $batch . "\n");
    print ($map_work_time / $batch . "\n");
    print ($map_user_time / $batch . "\n");
    print ($map_combiner_time / $batch . "\n");
    print (($map_time - ($map_work_time + $map_user_time + $map_combiner_time)) / $batch . "\n");
    print ($reduce_work_time / $batch . "\n");
    print ($reduce_user_time / $batch . "\n");
    print (($reduce_time - ($reduce_work_time + $reduce_user_time)) / $batch . "\n");
    print ($merge_work_time / $batch . "\n");
    print (($merge_time - $merge_work_time) / $batch . "\n");
    print ($library_fin_time / $batch . "\n");
    print ($inter_library_time / $batch . "\n");
    print ($fin_time / $batch . "\n");
}
