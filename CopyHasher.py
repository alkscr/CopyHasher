VERSION = "CopyHasher v0.1.240110"  # major.minor.date


from blake3 import blake3
from typing import Optional
import os
import sys
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    DownloadColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    TaskID,
)

def copy_file(
        total_bar: TaskID,
        file_bar: TaskID,
        in_file_path: str, 
        out_file_path: Optional[str] = None, 
        buffer_size: int = 24*(2**10)**2, 
        hasher=blake3()
) -> str:
    hasher.reset()

    in_file = open(in_file_path, 'rb')
    if not out_file_path == None:
        out_file = open(out_file_path, 'wb')
    else:
        out_file = None
    
    # read from input
    while b := in_file.read(buffer_size):
        read_size = len(b)
        progress.update(total_bar, advance=read_size)
        progress.update(file_bar, advance=read_size)
        hasher.update(b)
        # write if output exist
        if not out_file == None:
            out_file.write(b)

    # close file
    in_file.close()
    if not out_file == None:
        out_file.close()

    # output hash
    return hasher.hexdigest(512//8).upper()
    

def batch_process(
        progress: Progress,
        total_size: int,
        input_file_list: list[tuple[str,int]],
        input_root_dir: str,
        output_root_dir: Optional[str] = None,
        hasher = blake3()
):
    # open hash file
    hash_file_path = os.path.join(os.path.dirname(output_root_dir),
                                  os.path.basename(output_root_dir)+'.blake3-512' if not output_root_dir[-1] == '.'
                                  else 'checksum.blake3-512') \
                        if not output_root_dir == None \
                        else input_root_dir+'.blake3-512'
    hash_file = open(hash_file_path, 'w', encoding='UTF-8')
    hash_file.write('# ' + VERSION + '\n')
    hash_file.write('# blake3-512\n')

    # add total progress bar
    assert isinstance(progress, Progress)
    assert isinstance(total_size, int)
    total_bar = progress.add_task(
        'Copying' if not output_root_dir == None else 'Hashing',
        total = total_size,
        num_finished = 0,
        num_total = len(input_file_list)
    )
    file_bar = progress.add_task(
        'File',
        start = False,
        num_finished = 0,
        num_total = 1
    )

    # process file
    finished_count = 0
    for input_file, file_size in input_file_list:
        # reset file_bar
        progress.reset(file_bar, total=file_size, num_finished=0, num_total=1)
        progress.console.log(os.path.basename(input_file))

        # start progressing file
        input_file_path = os.path.join(input_root_dir, input_file)
        output_file_path = os.path.join(output_root_dir, input_file) if not output_root_dir == None else None
        # check and create output path
        output_file_dir = os.path.dirname(output_file_path)
        os.makedirs(output_file_dir, exist_ok=True)
        # copy file if output dir isn't None, and get hash
        hash_str = copy_file(total_bar, file_bar, input_file_path, output_file_path, hasher=hasher)
        finished_count += 1
        progress.update(total_bar, num_finished=finished_count)
        progress.update(file_bar, num_finished=1)

        # generate hash info
        hash_path = os.path.join(os.path.basename(output_root_dir), input_file) \
                    if not output_root_dir == None \
                    else os.path.join(os.path.basename(input_root_dir), input_file)
        hash_line = hash_str + ' *' + hash_path + '\n'
        # write hash info
        hash_file.write(hash_line)

    hash_file.close()


def walk_dir(
        in_dir: str
) -> tuple[list[tuple[str,int]],int]:
    file_list = [] # [(file_path, file_size), ...]
    total_size = 0
    for n in os.walk(in_dir):
        if not len(n[2]) == 0:
            for f in n[2]:
                file_path = os.path.join(n[0],f)
                file_size = os.path.getsize(file_path)
                # remove root path
                file_path = file_path[len(os.path.commonpath([in_dir,file_path]))+1:]
                file_list.append((file_path, file_size))
                total_size += file_size
    return file_list, total_size

if __name__ == '__main__':
    input_path = sys.argv[1] if len(sys.argv) >= 2 else None
    output_path = sys.argv[2] if len(sys.argv) >= 3 else None

    assert isinstance(input_path, str)

    file_list = []
    total_size = 0
    input_root = None
    output_root = None
    # process every thing in the folder
    if input_path[-1] == '\\':
        temp_list, total_size = walk_dir(input_path)
        file_list.extend(temp_list)
        input_root = input_path[:-1]
    # if input path is dir
    elif os.path.isdir(input_path):
        temp_list, total_size = walk_dir(input_path)
        file_list.extend(temp_list)
        input_root = input_path
    # if input path is file
    else:
        total_size = os.path.getsize(input_path)
        file_list.append(os.path.basename(input_path))
        input_root = os.path.dirname(input_path)

    # if no output
    if output_path == None:
        output_root = None
    # if output into the dir
    elif output_path[-1] == '\\':
        if input_path[-1] == '\\':
            output_root = output_path + '.'
        else:
            output_root = os.path.join(output_path, os.path.basename(input_root))
    # rename output dir (renaming file is not allowed, this will cause creating a new folder)
    else:
        output_root = output_path

    print(os.path.basename(input_root))
    print(output_root)

    # define progress bar
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        "•",
        DownloadColumn(binary_units=True),
        "•",
        "[progress.percentage]{task.fields[num_finished]:d}/{task.fields[num_total]:d}",
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    with progress:
        batch_process(progress, total_size, file_list, input_root, output_root, blake3())
    