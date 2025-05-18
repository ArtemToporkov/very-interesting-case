import os

def gather_files_to_txt(root_folder, output_filename="combined_code.txt", ignore_dirs=None, ignore_extensions=None,
                        ignore_files=None):
    """
    Собирает все текстовые файлы из указанной папки и ее подпапок в один TXT-файл.
    Перед содержимым каждого файла добавляется комментарий с его относительным путем.

    :param root_folder: Корневая папка для поиска файлов.
    :param output_filename: Имя итогового TXT-файла.
    :param ignore_dirs: Список имен папок, которые нужно игнорировать (например, ['.git', '__pycache__']).
    :param ignore_extensions: Список расширений файлов, которые нужно игнорировать (например, ['.pyc', '.log']).
    :param ignore_files: Список имен файлов, которые нужно игнорировать (например, ['output.txt']).
    """
    if ignore_dirs is None:
        ignore_dirs = ['.git', '__pycache__', 'node_modules', '.venv', 'venv', 'build', 'dist', '.rasa', 'rasa']
    if ignore_extensions is None:
        # По умолчанию пытаемся взять большинство текстовых/кодовых файлов, но можно настроить
        ignore_extensions = [
            '.pyc', '.pyo', '.pyd', '.so', '.dll', '.exe', '.o', '.a', '.lib',  # скомпилированные
            '.log', '.tmp', '.bak', '.swp', '.swo',  # временные/логи
            '.DS_Store', '.Spotlight-V100', '.Trashes', 'ehthumbs.db', 'Thumbs.db',  # системные
            '.gz', '.zip', '.tar', '.rar', '.7z',  # архивы
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg', '.ico',  # изображения
            '.mp3', '.wav', '.ogg', '.flac', '.aac',  # аудио
            '.mp4', '.mov', '.avi', '.mkv', '.webm',  # видео
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.odt', '.ods', '.odp',  # документы
            '.tar.gz', '.tar.bz2', '.tar.xz',
            '.gitignore'# архивы
        ]
    if ignore_files is None:
        ignore_files = []

    # Добавляем сам выходной файл в список игнорируемых, чтобы он сам себя не добавил
    # если запускать скрипт из той же папки, где он будет создан
    base_output_filename = os.path.basename(output_filename)
    if base_output_filename not in ignore_files:
        ignore_files.append(base_output_filename)

    if not os.path.isdir(root_folder):
        print(f"Ошибка: Папка '{root_folder}' не найдена.")
        return

    collected_files_count = 0
    output_abs_path = os.path.abspath(os.path.join(root_folder, output_filename))
    # Если output_filename содержит путь, используем его, иначе создаем в root_folder
    if os.path.dirname(output_filename):
        output_abs_path = os.path.abspath(output_filename)
    else:
        output_abs_path = os.path.abspath(os.path.join(root_folder, output_filename))

    with open(output_abs_path, 'w', encoding='utf-8') as outfile:
        for dirpath, dirnames, filenames in os.walk(root_folder):
            # Исключаем папки из дальнейшего обхода
            dirnames[:] = [d for d in dirnames if d not in ignore_dirs]

            for filename in filenames:
                if filename in ignore_files:
                    # print(f"Пропуск файла (в списке игнорируемых): {filename}")
                    continue

                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext in ignore_extensions:
                    # print(f"Пропуск файла (расширение в списке игнорируемых): {filename}")
                    continue


                full_path = os.path.join(dirpath, filename)

                # Проверяем, не является ли это сам выходной файл (на случай если он уже существует)
                if os.path.abspath(full_path) == output_abs_path:
                    continue

                relative_path = os.path.relpath(full_path, root_folder)
                # Заменяем разделители пути Windows на Unix-подобные для единообразия
                comment_path = relative_path.replace(os.sep, '/')

                try:
                    with open(full_path, 'r', encoding='utf-8', errors='ignore') as infile:
                        content = infile.read()

                    outfile.write(f"# Файл: {comment_path}\n")
                    outfile.write("# Содержимое:\n")
                    outfile.write(content)
                    outfile.write("\n\n" + "=" * 80 + "\n\n")  # Разделитель между файлами
                    collected_files_count += 1
                    print(f"Добавлен: {relative_path}")

                except Exception as e:
                    # Если файл не текстовый или проблема с кодировкой, которую errors='ignore' не решила
                    print(
                        f"Не удалось прочитать файл (возможно, бинарный или проблема с кодировкой): {relative_path} - {e}")
                    outfile.write(f"# Файл (НЕ УДАЛОСЬ ПРОЧИТАТЬ): {comment_path}\n")
                    outfile.write(f"# Ошибка: {e}\n")
                    outfile.write("\n\n" + "=" * 80 + "\n\n")

    if collected_files_count > 0:
        print(f"\nГотово! {collected_files_count} файлов собрано в '{output_abs_path}'.")
    else:
        print(
            f"\nВ папке '{root_folder}' не найдено подходящих файлов (с учетом фильтров). Файл '{output_abs_path}' создан, но пуст или содержит только ошибки чтения.")


if __name__ == "__main__":
    # --- Настройки ---
    target_folder = r"D:\Document\SecCourseMATMEX\ml\34\very-interesting-case"  # Текущая папка. Можно указать абсолютный или относительный путь, например "my_project_folder"
    # target_folder = "C:/Users/YourUser/Projects/MyAwesomeProject"

    output_file_name = "project_code_combined.txt"  # Имя итогового файла

    # Список папок для игнорирования (дополните при необходимости)
    custom_ignore_dirs = ['.git', '__pycache__', 'node_modules', '.venv', 'venv', 'build', 'dist', 'target', '.idea',
                          '.vscode', '.rasa']

    # Список расширений для игнорирования (дополните при необходимости)
    # Расширения, которые точно не являются текстовым кодом или конфигурацией
    custom_ignore_extensions = [
        '.gz', '.gitignore', '.gitattributes'
        # '.pyc', '.pyo', '.pyd', '.so', '.dll', '.exe', '.o', '.a', '.lib',  # скомпилированные
        # '.log', '.tmp', '.bak', '.swp', '.swo',  # временные/логи
        # '.DS_Store', '.Spotlight-V100', '.Trashes', 'ehthumbs.db', 'Thumbs.db',  # системные
        # '.gz', '.zip', '.tar', '.rar', '.7z',  # архивы
        # '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg', '.ico',  # изображения
        # '.mp3', '.wav', '.ogg', '.flac', '.aac',  # аудио
        # '.mp4', '.mov', '.avi', '.mkv', '.webm',  # видео
        # '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.odt', '.ods', '.odp',  # документы
        # '.jar', '.war',  # Java архивы
        # '.woff', '.woff2', '.ttf', '.eot',  # шрифты
    ]
    # Если хотите наоборот, указать только те расширения, которые включать,
    # то этот скрипт нужно будет немного доработать. Текущая логика - исключение.

    # Список конкретных файлов для игнорирования
    custom_ignore_files = [output_file_name] + ['.gitattributes', '.gitignore'] # Автоматически добавится, но можно добавить и свои
    # custom_ignore_files.append("README.md") # Например, если не хотите включать README

    # --- Запуск ---
    print(f"Сбор файлов из папки: {os.path.abspath(target_folder)}")
    print(
        f"Результат будет сохранен в: {os.path.abspath(os.path.join(target_folder, output_file_name)) if not os.path.dirname(output_file_name) else os.path.abspath(output_file_name)}")

    gather_files_to_txt(
        target_folder,
        output_file_name,
        ignore_dirs=custom_ignore_dirs,
        ignore_extensions=custom_ignore_extensions,
        ignore_files=custom_ignore_files
    )