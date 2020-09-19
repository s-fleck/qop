# qop - Queued file operation

qop (pronounced like "cop") is a file transfer tool that is optimized for copying or transcoding audio files to mobile 
media players, where connection speed, drive space or audio format support are an issue.

qop manages a single copy process in the background, so you do not have to worry about large numbers of transfers
spamming your legacy USB connections. If your device gets disconnected for whatever reason, qop retains the last
transfer queue (or an arbitrary number of named queues if you desire) and it can be resumed whenever you wish. qop
can also smartly transcode your 192khz 24bit audiophile flac files to something more appropriate for your portable music
player, while leaving your old mp3 files from the 90is untouched.


## Features:

* Conditional audio transcoding (e.g. transcode only lossless files) 
* Tag cleanup (e.g. remove album covers) [planned]
* Multicore audio transcoding [wip] 
* Persistent job-queues to resume aborted transfers 
* Clean and powerful CLI


## Development status:

qop is in an alpha stage and not fit for general use.


# usage examples

```
# queued operations
qop copy file1 file2 /my/audio/dir
qop move file1 file2 /my/audio/dir
qop convert file1 file2 /my/audio/dir

# practical example:
# copy mp3s and transcode flacs to an mp3 player, ignoring all files that are not mp3s or flacs
qop convert song.mp3 fugue.flac cover.jpg /mnt/mp3player --convert-only flac --include mp3 flac

# repeat the last command for your whole music directory (with the same output directory) 
qop re ~/music
```

# architecture

qop consists of two programs: *qop*, the command line client and *qopd*, the daemon. The daemon processes
the transfer queue and executes the copy and transcode tasks, while the client can be used to manage and monitor 
the daemon (enqueue new files, start and stop processing the queue, monitor queue progress, etc...). The daemon
can be launched manually for greater control, but if no daemon process is found the client creates its own
process. Transfer queues are stored as json strings in sqlite3 databases, and if you are familiar with these 
technologies you can easily create transfer queues without relying on the CLI client.
 