import shutil

def backup_bd():
  original = r'database/datab.db'
  target = r'backup/datab.db'
  shutil.copyfile(original, target)

  original = r'database/datab.db'
  target = r'backup/datab.db'
  shutil.copyfile(original, target)

  src = r'migrations'
  dest = r'backup/migrations'
  shutil.rmtree(r'backup/migrations')
  shutil.copytree(src, dest)
  

backup_bd()  