from sorl.thumbnail.conf import settings, defaults as default_settings
from sorl.thumbnail.helpers import tokey, serialize
from sorl.thumbnail.images import DummyImageFile, ImageFile
from sorl.thumbnail import default
from sorl.thumbnail.parsers import parse_geometry
from sorl.thumbnail.base import ThumbnailBackend

from sorlery.tasks import create_thumbnail


class QueuedThumbnailBackend(ThumbnailBackend):
    """
    Queue thumbnail generation with django-celery.
    """
    def get_thumbnail(self, file_, geometry_string, **options):
        source = ImageFile(file_)
        for key, value in self.default_options.iteritems():
            options.setdefault(key, value)
        # For the future I think it is better to add options only if they
        # differ from the default settings as below. This will ensure the same
        # filenames beeing generated for new options at default.
        for key, attr in self.extra_options:
            value = getattr(settings, attr)
            if value != getattr(default_settings, attr):
                options.setdefault(key, value)
        name = self._get_thumbnail_filename(source, geometry_string, options)
        thumbnail = ImageFile(name, default.storage)
        cached = default.kvstore.get(thumbnail)
        if cached:
            return cached

        # We cannot check if the file exists, as remote storage is slow. If
        # we have reached this point, the image does not exist in our kvstore
        # so create the entry and queue the generation of the image.
        #
        # Note: If the thumbnail file has been deleted, you will need to manually
        # clear the corresponding row from the kvstore to have thumbnail rebuilt.
        job = create_thumbnail.delay(file_, geometry_string, options, name)
        if job:
            # We don't want to do any file access in this thread, so we need
            # to create dummy images for the thumbnail and the image that will
            # be replaced in the queued job.
            #
            # I wasn't able to get this working with the key and serialisedstorage stuff,
            # so for the moment it simply returns dummy images on the first request
            # and a 404 on all future requests until the thumbnail is built.
            temp_thumbnail = TemporaryDummyImageFile(geometry_string)
            thumbnail.set_size(parse_geometry(geometry_string))

            # We can't add a source row to the kvstore without the size
            # information being looked up, so add dummy information here
            # We'll need to correct this information when we generate the thumbnail
            source.set_size(parse_geometry(geometry_string))

            default.kvstore.get_or_set(source)
            default.kvstore.set(thumbnail, source)

        return temp_thumbnail


class TemporaryDummyImageFile(DummyImageFile):
    pass

