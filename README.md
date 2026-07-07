# ballast

Finds the skew that makes your Spark jobs list.

A Spark stage is only as fast as its slowest task, and in every aging
pipeline there is a stage where one hot key hands one task 80% of the
shuffle. The other 199 executor slots finish in a minute and then sit,
paid for and idle, while the straggler grinds. The Spark UI shows you this
one stage at a time, if you know to look.

ballast reads the event logs Spark already writes and reads across all of
it: stragglers, partition skew, spill, GC pressure, and the executor-hours
the imbalance wastes.

Early days: the event-log parser lands first.

MIT.
