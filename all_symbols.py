import os

def read_raw_dir(dir_path):
    data = []
    if os.path.isdir(dir_path) == False:
        print(f"{dir_path} not found")
        return data
    print(f"Reading from: {dir_path} directory")
    for filename in os.listdir(dir_path):
        path = os.path.join(dir_path, filename)

        if os.path.isfile(path) == False:
            print('Error: Not a file' + path)
            continue

        with open(path) as f:
            # read first line to remove header
            first_line = f.readline()
            print(f"Skipping this line: {first_line}")
            for line in f:
                data.append("".join(line.split()).split(',')[0])
    return data

def read_clean_dir(dir_path):
    data = []
    if os.path.isdir(dir_path) == False:
        print(f"{dir_path} not found")
        return data
    print(f"Reading from: {dir_path} directory")
    for filename in os.listdir(dir_path):
        path = os.path.join(dir_path, filename)

        if os.path.isfile(path) == False:
            print('Error: Not a file' + path)
            continue

        with open(path) as f:
            line = f.readline()
            all_symbols = "".join(line.split()).split(',')
            data.append(all_symbols)

    # flatten list of lists
    flat_list = [item for sublist in data for item in sublist]

    while('' in flat_list) :
        flat_list.remove('')
    return flat_list

def print_to_file(filename, symbols):
    with open(filename, 'w') as f:
        f.writelines("%s," % symbol for symbol in symbols)
    with open(filename, 'rb+') as f:
        f.seek(-1, os.SEEK_END)
        f.truncate()

def get_unique_symbols(symbols):
    list_set = set(symbols)
    # convert the set to the list
    new_list = list(list_set)
    return new_list

def main():
    dir_type = input("Would you like to read from 'raw' or 'clean': ")
    if dir_type != "raw" and dir_type != "clean":
        print('Error: You must select either raw or clean')
        import sys
        sys.exit(1)
    dir_path = f"data/{dir_type}/symbols"
    if dir_type == 'raw':
        symbols = read_raw_dir(dir_path)
        output_filename = os.path.join(dir_path, 'all_symbols.csv')
    elif dir_type == 'clean':
        symbols = read_clean_dir(dir_path)
        output_filename = os.path.join(dir_path, 'final_all_symbols.csv')
    print(f"Reading symbols from the following directory: {dir_path}\nOutput path: {output_filename}")
    if symbols:
        print('Number of symbols read: ' + str(len(symbols)))
        unique_symbols = get_unique_symbols(symbols)
        print('Number of unique symbols read: ' + str(len(unique_symbols)))
        print_to_file(output_filename, unique_symbols)

if __name__ == '__main__':
    main()
