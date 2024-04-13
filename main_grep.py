import pandas as pd
import re
import matplotlib.pyplot as plt
import glob

def plot_csv_to_png(csv_file_path, output_png_path):
    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Plot the data
    plt.plot(df.index, df['values'])
    plt.xlabel('Index')
    plt.ylabel('Values')
    plt.title('Graph for {}'.format(csv_file_path))

    # Save the plot as a PNG file
    plt.savefig(output_png_path)
    plt.close()

def grep_files(input_file, pattern, output_file):
    with open(input_file, 'r') as input_file:
        with open(output_file, 'w') as output_file:
            for line in input_file:
                if pattern in line:
                    # output_file.write(f"Found '{pattern}' in file: {input_file.name}\n")
                    output_file.write(line.rstrip() + '\n')  # Write the line containing the pattern

def filter_lines(file_path, pattern):
    # Open the file in read mode
    with open(file_path, 'r') as file:
        lines = file.readlines()  # Read all lines into a list

    # Filter out lines containing the pattern
    filtered_lines = [line for line in lines if pattern not in line]

    # Open the file again in write mode to overwrite it with filtered content
    with open(file_path, 'w') as file:
        file.writelines(filtered_lines)

def text_to_csv(input_file_path, output_csv_path):
    # Read the text file into a DataFrame
    try:
        df = pd.read_csv(input_file_path, header=None, names=['column_name'])
    except:
        print("toto")

    # Extract the function and value from the 'column_name' column
    df[['function', 'values']] = df['column_name'].str.split(expand=True)
    df['values'] = df['values'].astype(float)

    # Drop the original 'column_name' column
    df.drop(columns=['column_name'], inplace=True)

    # Reset index starting from 0
    df.reset_index(drop=True, inplace=True)

    # Save the DataFrame to a CSV file
    df.to_csv(output_csv_path, index=True)

    print("DataFrame created and saved to '{}'".format(output_csv_path))


def text_to_csv_requests(input_file_path, output_csv_path):
    # Read the text file into a DataFrame
    df = pd.read_csv(input_file_path, header=None, names=['text'])

    # Extract the 'requests' and 'value' from the 'text' column
    df['requests'] = df['text'].str.extract(r'requests: (\d+\.\d+)').astype(float)
    df.drop(columns=['text'], inplace=True)
    df["values"] = df['requests'].copy()
    df.drop(columns=['requests'], inplace=True)
    df["function"] = "requests"

    # Reset index starting from 0
    df.reset_index(drop=True, inplace=True)

    # Save the DataFrame to a CSV file
    df.to_csv(output_csv_path, index=True)

    print("DataFrame created and saved to '{}'".format(output_csv_path))

# Usage example
input_file = './test_time_tmp/log_test_request.txt'  # Specify the filename of the input file
pattern_to_remove = "Found"

lst_fct = ["get_open_position", "get_open_orders",
           "get_values", "get_df_account",
           "elapsed_time",
           "open_long_order", "cancel_open_ong_orders",
           "close_long_order", "cancel_close_long_orders"]



for fct in lst_fct:
    if fct == "elapsed_time":
        print(fct)
    pattern = fct  # Specify the pattern to search for
    output_file = './test_time_tmp/' + fct + '.txt'  # Specify the filename for the output
    output_file_csv = './test_time_tmp/' + fct + '.csv'

    grep_files(input_file, pattern, output_file)
    # filter_lines(output_file, pattern_to_remove)
    if fct == "elapsed_time":
        text_to_csv_requests(output_file, output_file_csv)
    else:
        text_to_csv(output_file, output_file_csv)

# Directory containing CSV files
csv_directory = './test_time_tmp'

# Iterate over each CSV file in the directory
for csv_file in glob.glob(csv_directory + '/*.csv'):
    # Define the output PNG file path
    output_png_file = csv_file.replace('.csv', '.png')

    # Plot the CSV data and save as PNG
    plot_csv_to_png(csv_file, output_png_file)