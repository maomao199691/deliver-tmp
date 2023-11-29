import tkinter as tk
from tkinter import messagebox

def test(e):
    '''创建弹窗'''
    return_value = messagebox.showinfo('窗口名称', '点击成功')
    print(type(return_value), return_value)


if __name__ == '__main__':
    root = tk.Tk()

    root.title('测试窗口')
    root.geometry("800x500+260+80") # (宽度x高度)+(x轴+y轴)

    btn1 = tk.Button(root)

    btn1['text'] = '按钮1'
    btn1.place(relx= 0.2, x=50, y=20, relwidth=0.05, relheight=0.1)
    btn2 = tk.Button(root)
    btn2['text'] = '按钮2'
    btn2.grid(column=0, row=1, ipady=20)

    btn2.bind("<Button-1>", test)



    root.mainloop()