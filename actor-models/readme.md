I've tried different to push `go` to process hundread of millions of messages but... it takes too much cpu to process 300M messages. You need multiple readers and multipler producers.

After profiling it a bit, i deduct that the memory model of `go` make it impossible to have a really efficient memory access without being disturbed by GC or else.

Either `go` will need more memory control with less GC disruptions or it will require a fine tune control over the storage and access of messages. 

I'm willing to push the experiment further but i will be forced to use `CGO` ¯\_(ツ)_/¯

