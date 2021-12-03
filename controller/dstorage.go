package controller

//use rate limiter here.
//All upload should go through this file so you can limit rate of upload request so you don't get blocked by blobber.
//Its better to put rate limit value in some variable; check rate limit of all blobbers and put rate limit value of the blobber that has minimum capacity.
//While this file helps to rate limit; there might be goroutine leak in migrate.go so that we need to process uploads in batch.
//
//Batch is simpler to use than the continuous upload.
//Concept is you take a bunch of s3 objects in batch and wait until all the objects from this batch is uploaded. If any upload fails then terminate migration of this bucket.
//let other bucket operate.
//This way you can update state for each bucket;

//We also need to be careful about committing upload. There might be race between committing request resulting in commit failure.
//So lets put commit request in a queue(use channel) and try three times. If it fails to commit then save state of all bucket and abort the program.
