bulk upload images and vids to imgchest/catbox.
This is intended to be used with boards_v2, this basically uploads images in bulk if you want to upload all of your stuff at once.
Try not to abuse it, as you may get banned and lose images.

I tried using gofile but didn't really work.

get an api token from imgchest. (catbox doesn't "need" an api token. if you use an api token, you can view your saved images in your account.)

put them in a .env file.

IMG_CHEST_API_KEY=[replace with key]


# todo
- [ ] make it so that the leafs are uploaded as a single imgchest post, even if it takes multiple requests to do so. use the stored imgchest post id. 
