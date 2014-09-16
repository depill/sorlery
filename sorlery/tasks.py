from celery import shared_task
from sorl.thumbnail import default
from sorl.thumbnail.images import ImageFile


@shared_task
def create_thumbnail(file_, geometry_string, options, name):
    thumbnail = ImageFile(name, default.storage)

    if not thumbnail.exists():
        if file_:
            source = ImageFile(file_)
        else:
            return
        source_image = default.engine.get_image(source)
        try:
            image_info = default.engine.get_image_info(source_image)
            options['image_info'] = image_info
        except AttributeError:
            options['image_info'] = {}
        size = default.engine.get_image_size(source_image)
        source.set_size(size)
        default.backend._create_thumbnail(source_image, geometry_string, options, thumbnail)

        # Need to update both the source and the thumbnail with correct sizing
        default.kvstore.set(source)
        default.kvstore.set(thumbnail, source)
