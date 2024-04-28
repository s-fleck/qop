# Queued file operation 

qop (pronounced like "cop") is a file transfer tool optimized for copying and transcoding audio files to mobile 
media players where connection speed, drive space, or audio format support are limited.

qop manages a single transfer process in the background because large numbers of concurrent transfers can clog USB 
connections and provide no advantage for total transfer speed. If your device gets disconnected, qop retains transfer 
queues that can be resume whenever desired. qop can also smartly transcode files based on their format. For example, 
qop can transcode your 192khz 48bit audiophile flac files to mp3 before sending them to your media device  
while leaving files that are already in a lossy format untouched.


## Features

* Conditional audio transcoding; e.g. transcode only lossless files. 
* Multicore support to speed up audio transcoding.
* Remove cover art tags on-the-fly. Useful because some mobile phones display art from audio tags in their gallery, which can be annoying.  
* Persistent job-queues to resume aborted transfers 
* Clean and powerful CLI


## Development status

qop is functional but not super polished. It's in maintancne mode so don't expect any new features.

## Usage 

```
# basic usage
qop copy file1 file2 /media/mp3player
qop move file1 file2 /media/mp3player
qop convert file1 file2 /media/mp3player

# repeat the last command with the same paramters and output directory on different inputs
qop re file3 file4

# examples: 

# copy two songs to a portable media player
# - inlcude only flac and mp3 files (ignore cover.jpg)
# - transcode flac files and leave mp3 files untouched
# - by default convert produces VBR mp3s of the highest quality (lame V0).
qop convert song.mp3 fugue.flac cover.jpg /media/mp3player --include mp3 flac --convert-only flac 

# repeat the last command for your whole music directory
qop re ~/music

# copy files to a media player and remove album art tags (without audio-transcoding)
qop convert * /media/m3player --convert-none --remove-art  

# display a progress bar
qop progress 

# show more examples
qop --examples
```


## Architecture

qop consists of two programs: 

- *qopd*, a daemon which processes the transfer queue and executes the copy and transcode tasks, and 
- *qop*, a command line client which can put tasks into the queue, tell the daemon to start or stop processing the queue, monitor transfer progress, etc...
    
Transfer queues are stored as json strings in sqlite3 databases. If you are familiar with these 
technologies you can easily create transfer queues from the scripting language of your choice without needing to
go through the cli program. For more details please refer to the [api documentation](https://s-fleck.github.io/qop/).
 
