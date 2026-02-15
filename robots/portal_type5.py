def exec5(cities_config, downloads_folder, state, progress_callback=None):
    if progress_callback:
        for _ in cities_config:
            progress_callback()
    return