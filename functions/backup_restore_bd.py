import shutil

def backup_bd():
  original = r'database/datab.db'
  target = r'backup/datab.db'
  shutil.copyfile(original, target)

def restore_bd():
  original = r'backup/datab.db'
  target = r'database/datab.db'
  shutil.copyfile(original, target)