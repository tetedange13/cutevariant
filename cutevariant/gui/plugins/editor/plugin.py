from cutevariant.gui import plugin

from PySide2.QtWidgets import *
import sys
import sqlite3

from cutevariant.gui.plugins.editor import widget


class EditorPlugin(plugin.Plugin):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.editor = widget.VqlEditor()
        self.dockable = False
        self.editor.executed.connect(self.on_vql_executed)

    def get_widget(self):
        """Overload from Plugin
        
        Returns:
            QWidget
        """
        return self.editor

    def on_open_project(self, conn):
        """Overload from Plugin
        
        Arguments:
            conn 
        """
        self.editor.conn = conn

    def on_query_model_changed(self):
        self.editor.set_vql(self.mainwindow.query_widget.model.query.to_vql())

    def on_vql_executed(self):
        self.mainwindow.query_widget.model.columns = self.editor.columns
        self.mainwindow.query_widget.model.filter = self.editor.filter
        self.mainwindow.query_widget.model.selection = self.editor.selection
        self.mainwindow.query_widget.model.load()


if __name__ == "__main__":

    app = QApplication(sys.argv)

    p = EditorPlugin()

    w = p.get_widget()

    w.show()

    app.exec_()