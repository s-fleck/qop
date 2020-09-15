# qop - Queued file operation

A simple file-transfer tool optimized for moving audio files to mobile devices.
qop uses persistent transfer-queues which makes it easy to resume aborted transfers for slow or 
unreliable connections (e.g. some mobile phones or old mp3 players). qop also supports on-the-fly audio
transcoding which makes it possibly to transcode lossless formats (such as FLAC) to mp3 or ogg, while 
leaving lossy formats untouched.

## Features:

* Conditional audio transcoding (e.g. transcode only lossless files) 
* Tag cleanup (e.g. remove album covers) [planned]
* Multicore support [wip]
* Persistent job-queues to resume aborted transfers 
* Clean and simple CLI


## Development status:

qop is in an alpha stage and not fit for general use.


# usage examples

```
# queued operations
qfop copy file1 file2 /my/audio/dir
qfop delete file1 file2
qfop move file1 file2 /my/audio/dir
qfop convert file file --profile
qfop echo msg 

# commands
qfop flush  # reset the queue
qfop info   
qfop progress
qfop start
qfop pause
```
