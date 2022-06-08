
import sys
import pipeline
import messages

sys.argv=["--skipLeadingRows=1"]
#exec('pipeline.py')
#subprocess.call(["python3 pipeline.py","--skipLeadingRows=1"])
pipeline.run()
messages.run()