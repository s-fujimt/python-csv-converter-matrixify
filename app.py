import PySimpleGUI as sg
import os.path
import pandas as pd
import sys
from data import COLUMNS, FILES_NEEDED, FILE_PATHS, DATAFRAMES
from merge import mergeFiles
import csv


def check_if_csv_is_valid(f):
    with open(f, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        data_rows = [row for row in reader]
        max_data_len = max([len(row) for row in data_rows])

    if len(headers) != max_data_len:
        diff = abs(len(headers) - max_data_len)
        for i in range(diff):
            headers.append("")

        with open(f, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(data_rows)


sg.theme("LightGray1")
OUTPUT_DATAFRAME = pd.DataFrame()

file_select = [
    [
        sg.Text("CSVファイルを選択してください", font=("Helvetica", 12, "bold")),
    ],
    [
        sg.Input(key="-FILE LIST-", enable_events=True, visible=False),
        sg.FilesBrowse("ファイルを選択", target="-FILE LIST-",
                       file_types=(("CSV Files", "*.csv"),), enable_events=True),
        sg.Text('複数ファイル選択可')
    ],
]

file_rows = ([sg.Text(file["label"]), sg.Text("", key="-"+file["name"].upper()+"-", font=("bold")), sg.Button(
    "削除", key="-UNSELECT "+file["name"].upper()+"-", visible=False)] for file in FILES_NEEDED)

file_check = [
    [sg.Text("")],
    [sg.Text("下記ファイルを選択してください", font=("Helvetica", 10, "bold"))],
    *file_rows,
    [sg.Text("", key="-TOUT-")],
]

layout = [
    file_select,
    file_check,
    [sg.Button("マージ", key="-MERGE-", visible=False),
     sg.Button("ファイルに保存", key="-SAVE-", visible=False)],
    [sg.Multiline(key="-TOUT2-", size=(100, 20),
                  visible=False)],
]

window = sg.Window("CSV変換", layout, size=(600, 500), margins=(20, 20))


while True:
    event, values = window.read()

    if event in ["Exit", sg.WIN_CLOSED]:
        break

    if event.startswith("-UNSELECT "):
        file_type = event.split("-")[1].replace("UNSELECT ", "")
        window[f"-{file_type}-"].update("")
        window[event].update(visible=False)
        FILE_PATHS[file_type.lower()] = ""
        DATAFRAMES[file_type.lower()] = pd.DataFrame()

        if not all(FILE_PATHS.values()):
            window["-MERGE-"].update(visible=False)

    elif event == "-FILE LIST-":
        try:
            for f in values["-FILE LIST-"].split(";"):
                uploaded_filename = os.path.basename(f)

                if uploaded_filename in [file["filename"] for file in FILES_NEEDED]:
                    check_if_csv_is_valid(f)
                    file_type = [file["name"] for file in FILES_NEEDED if file["filename"] ==
                                 uploaded_filename][0]
                    DATAFRAMES[file_type] = pd.read_csv(f)

                    if not all(elem in DATAFRAMES[file_type].columns for elem in COLUMNS[file_type]):
                        window[f"-{file_type.upper()}-"].update(
                            "✗ File Invalid", text_color="red")
                    else:
                        FILE_PATHS[file_type] = f
                        DATAFRAMES[file_type] = DATAFRAMES[file_type][COLUMNS[file_type]]
                        window[f"-{file_type.upper()}-"].update("✓",
                                                                text_color="green")
                        window[f"-UNSELECT {file_type.upper()}-"].update(visible=True)

                else:
                    window["-TOUT-"].update(
                        f"Invalid File: {uploaded_filename}")

                if all(FILE_PATHS.values()):
                    window["-MERGE-"].update(visible=True)

        except:
            print("Error", sys.exc_info()[0], sys.exc_info()[1])
            pass

    if event == "-MERGE-":
        result = mergeFiles()
        if result['error']:
            window["-TOUT2-"].update("マージ失敗")
            window["-SAVE-"].update(visible=False)
            break

        OUTPUT_DATAFRAME = result['data']
        window["-TOUT2-"].update("マージ成功\n"+str(len(OUTPUT_DATAFRAME.columns)) +
                                 "カラム\n"+str(len(OUTPUT_DATAFRAME)) + "件", visible=True)
        window["-SAVE-"].update(visible=True)

    if event == "-SAVE-":
        output_path = sg.popup_get_file("出力ファイルを保存", save_as=True, file_types=(
            ("CSV Files", "*.csv"),), no_window=True, default_path="Products.csv")
        if output_path:
            OUTPUT_DATAFRAME.to_csv(output_path, index=False)
            sg.popup("出力ファイルを保存しました")

window.close()
