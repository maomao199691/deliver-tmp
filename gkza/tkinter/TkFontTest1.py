import tkinter as tk
import tkinter.font as font


if __name__ == '__main__':
    root = tk.Tk()

    root.title('字体测试窗口')
    root.geometry("800x500+260+80")  # (宽度x高度)+(x轴+y轴)

    font_1 = font.Font(family='Helvetica', size=30, weight='normal')
    font_2 = font.Font(family='Arial', size=15, weight='bold')

    # bg: 背景颜色
    # foreground: 文件颜色
    but1 = tk.Button(root, text='背景色', font=font_1, bg="LightSkyBlue")
    but1.grid(row=0, column=0)

    label1 = tk.Label(root, text='文字颜色', font=font_2, foreground="Orange")
    label1.grid(row=0, column=2)

    root.mainloop()