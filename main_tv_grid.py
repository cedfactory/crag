import execute_time_recoder
import os
import shutil

lst_lines = [0.57, 0.5688333333333333, 0.5676666666666667, 0.5665, 0.5653333333333332, 0.5641666666666666, 0.563, 0.5618333333333333, 0.5606666666666666, 0.5595, 0.5583333333333333,
0.5571666666666666, 0.5559999999999999, 0.5548333333333333, 0.5536666666666666, 0.5525, 0.5513333333333333, 0.5501666666666666, 0.5489999999999999, 0.5478333333333333, 0.5466666666666666, 0.5455, 0.5443333333333333,
0.5431666666666667, 0.5419999999999999, 0.5408333333333333, 0.5396666666666666, 0.5385, 0.5373333333333333, 0.5361666666666667, 0.5349999999999999, 0.5338333333333333, 0.5326666666666666, 0.5315, 0.5303333333333333,
0.5291666666666667, 0.528, 0.5268333333333333, 0.5256666666666666, 0.5245, 0.5233333333333333, 0.5221666666666667, 0.521, 0.5198333333333334, 0.5186666666666666, 0.5175, 0.5163333333333333, 0.5151666666666667, 0.514,
0.5128333333333334, 0.5116666666666667, 0.5105, 0.5093333333333333, 0.5081666666666667, 0.507, 0.5058333333333334, 0.5046666666666667, 0.5035, 0.5023333333333333, 0.5011666666666666, 0.5]

print("// Liste des valeurs pour lesquelles des lignes horizontales doivent être tracées")
print("values = array.new_float(0)")

for line in lst_lines:
    print("array.push(values,", line,")")



"""
//@version=5
indicator("Lignes horizontales", overlay=true)


// Liste des valeurs pour lesquelles des lignes horizontales doivent être tracées
values = array.new_float(0)
array.push(values, 0.57)
array.push(values, 0.5688333333333333)
array.push(values, 0.5676666666666667)
array.push(values, 0.5665)
array.push(values, 0.5653333333333332)
array.push(values, 0.5641666666666666)
array.push(values, 0.563)
array.push(values, 0.5618333333333333)
array.push(values, 0.5606666666666666)
array.push(values, 0.5595)
array.push(values, 0.5583333333333333)
array.push(values, 0.5571666666666666)
array.push(values, 0.5559999999999999)
array.push(values, 0.5548333333333333)
array.push(values, 0.5536666666666666)
array.push(values, 0.5525)
array.push(values, 0.5513333333333333)
array.push(values, 0.5501666666666666)
array.push(values, 0.5489999999999999)
array.push(values, 0.5478333333333333)
array.push(values, 0.5466666666666666)
array.push(values, 0.5455)
array.push(values, 0.5443333333333333)
array.push(values, 0.5431666666666667)
array.push(values, 0.5419999999999999)
array.push(values, 0.5408333333333333)
array.push(values, 0.5396666666666666)
array.push(values, 0.5385)
array.push(values, 0.5373333333333333)
array.push(values, 0.5361666666666667)
array.push(values, 0.5349999999999999)
array.push(values, 0.5338333333333333)
array.push(values, 0.5326666666666666)
array.push(values, 0.5315)
array.push(values, 0.5303333333333333)
array.push(values, 0.5291666666666667)
array.push(values, 0.528)
array.push(values, 0.5268333333333333)
array.push(values, 0.5256666666666666)
array.push(values, 0.5245)
array.push(values, 0.5233333333333333)
array.push(values, 0.5221666666666667)
array.push(values, 0.521)
array.push(values, 0.5198333333333334)
array.push(values, 0.5186666666666666)
array.push(values, 0.5175)
array.push(values, 0.5163333333333333)
array.push(values, 0.5151666666666667)
array.push(values, 0.514)
array.push(values, 0.5128333333333334)
array.push(values, 0.5116666666666667)
array.push(values, 0.5105)
array.push(values, 0.5093333333333333)
array.push(values, 0.5081666666666667)
array.push(values, 0.507)
array.push(values, 0.5058333333333334)
array.push(values, 0.5046666666666667)
array.push(values, 0.5035)
array.push(values, 0.5023333333333333)
array.push(values, 0.5011666666666666)
array.push(values, 0.5)


// for i = 0 to array.size(values) - 1
//     val = array.get(values, i)
//    line.new(x1=bar_index - 500, y1=val, x2=bar_index + 500, y2=val, color=color.blue, width=1)

log.info("array size {0}", array.size(values))
for i = 0 to array.size(values) - 1
    val = array.get(values, i)
    line_color = val > close ? color.red : color.blue
    line.new(x1=bar_index - 500, y1=val, x2=bar_index + 500, y2=val, color=line_color, width=1)
    log.info("index {0} - Val: {1}", i, val)

// Ajouter une ligne jaune à 0.5260
line.new(x1=bar_index - 500, y1=0.5260, x2=bar_index + 500, y2=0.5260, color=color.yellow, width=1)

// Ajouter une ligne verticale orange à 16h23
// if (hour == 16 and minute == 23)
//     line.new(x1=bar_index, y1=low, x2=bar_index, y2=high, color=color.orange, width=2)

// Ajouter une ligne verticale orange le samedi 18 mai à 16h23
var int target_day = 22
var int target_month = 5
var int target_year = year
var int target_hour = 23
var int target_minute = 05

is_target_time = (dayofmonth == target_day and month == target_month and year == target_year and hour == target_hour and minute == target_minute)
if is_target_time
    line.new(x1=bar_index, y1=low, x2=bar_index, y2=high, color=color.orange, width=2)
"""