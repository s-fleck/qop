# qop - Queued file operation

qop (pronounced like "cop") is a file transfer tool that is optimized for copying or transcoding audio files to mobile 
media players, where connection speed, drive space or audio format support are an issue.

qop manages a single copy process in the background, so you do not have to worry about large numbers of transfers
spamming your legacy USB connections. If your device gets disconnected, qop retains transfer queues that can be resume whenever desired. qop
can also smartly transcode files based on their format. For example, qop can transcode your 192khz 48bit audiophile flac files to mp3 before sending them to a portable media player while, while leaving files that are already in a lossy format untouched.


## Features:

* Conditional audio transcoding (e.g. transcode only lossless files) 
* Tag cleanup (e.g. remove album covers) [planned]
* Multicore audio transcoding
* Persistent job-queues to resume aborted transfers 
* Clean and powerful CLI


## Development status:

qop is in a beta stage and under active development. While the core functionality is stable, the user interface is still beeing refined. A public release fit for general usage is planned for early 2021.


## Usage 

```
# queued operations
qop copy file1 file2 /my/audio/dir
qop move file1 file2 /my/audio/dir
qop convert file1 file2 /my/audio/dir

## practical example: ###
# - inlcude only flac and mp3 files (ignores cover.jpg)
# - conver only flac files, leave mp3 files untouched
qop convert song.mp3 fugue.flac cover.jpg /mnt/mp3player --include mp3 flac --convert-only flac 

# repeat the last command for your whole music directory
qop re ~/music
```

## Architecture

qop consists of two programs: 

- *qopd*, a daemon which processes the transfer queue and executes the copy and transcode tasks, and 
- *qop*, a command line client which can put tasks into the queue, tell the daemon to start or stop processing
    the queue, monitor transfer progress, etc...
    
Transfer queues are stored as json strings in sqlite3 databases. If you are familiar with these 
technologies you can easily create transfer queues from the scripting language of your choice without needing to
go through the cli program.
 
