from concurrent.futures import ThreadPoolExecutor

IMGCHEST_MAX_IMAGES = 20


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def upload_folder_to_imgchest(upload_func, headers, folder_name, image_paths):
    posts = []

    for batch in chunked(image_paths, IMGCHEST_MAX_IMAGES):
        result = upload_func(headers, folder_name, batch)
        posts.append(result)

    return posts


def upload_parallel(imgchest_callable, catbox_callable):
    with ThreadPoolExecutor(max_workers=2) as pool:
        img_future = pool.submit(imgchest_callable)
        cat_future = pool.submit(catbox_callable)

        return {
            "imgchest": img_future.result(),
            "catbox": cat_future.result(),
        }
