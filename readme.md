bulk upload images and vids to imgchest/catbox.
This is intended to be used with pixboards, this basically uploads images in bulk if you want to upload all of your stuff at once.
Try not to abuse it, as you may get banned and lose images.

Tried using gofile but didn't really work.

get an api token from imgchest. (catbox doesn't "need" an api token. if you use an api token, you can view your saved images in your account.)

put them in a .env file.

IMG_CHEST_API_KEY=[replace with key]

the newer commits chaged a lot, mainly how the uploads are done. 
Imgchest uploads happen folderwise instead of chunked uploads of 20 imgs each. 

# todo
- [ ] make it so that the leafs are uploaded as a single imgchest post, even if it takes multiple requests to do so. use the stored imgchest post id. 
- [ ] a more elegant solution for "use parent dir name for folders named 'gallery'"
