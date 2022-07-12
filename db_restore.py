import shutil

def restore_bd():
  original = r'backup/datab.db'
  target = r'database/datab.db'
  shutil.copyfile(original, target)

  src = r'backup/migrations'
  dest = r'migrations'
  shutil.rmtree(r'backup/migrations')
  shutil.copytree(src, dest)

restore_bd()