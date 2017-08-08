def strip_extension(gcs_filename, extension_list):
    replaced_extension = True
    while replaced_extension:
        replaced_extension = False
        for extension in extension_list:
            if not extension.startswith('.'):
                extension = '.' + extension
            if gcs_filename.endswith(extension):
                gcs_filename = gcs_filename[:-len(extension)]
                replaced_extension = True

    return gcs_filename
