
import queue
from com.sca.hem4.GuiThreaded import Hem4
import tkinter as tk
import tkinter.ttk as ttk

"""
Create the application and start it up.
"""



messageQueue = queue.Queue()
callbackQueue = queue.Queue()

#create window instance
window = tk.Tk()

#title
window.title("HEM4")


window.createWidgets()
window.mainloop()


hem4 = Hem4(window, messageQueue, callbackQueue)
#hem4.start_gui()