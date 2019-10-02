**Installation**:
  - Copy `AutoFFmpeg.py` and `AutoFFmpeg.param` to `<Deadline Repository>/events/AutoFFmpeg` folder.
  - Open menu `Tools - Configure Events..` in deadline monitor and choose `AutoFFmpeg` plugin from list and enable it.

**Configuration**:
  - *State*:
    - `Opt-In`: Plugin will be active only for jobs that specifies it in `Event Opt-Ins`.
    - `Global Enabled`: Work on any job.
    - `Disabled`: Disable plugin.
  - *Job Name Filter (python regular expression)*: Use python regex to filter job based on name. Default `.+` process any job.
  - *Plugin Name Filter (python regular expression)*: Use python regex to filter job based on plugin name. Default `.+` process any job.
  - *Input Args*: ffmpeg input arguments (NOTE: do not pass `-start_number` flag it handeled internally based on job frames list).
  - *Output Args*: ffmpeg output arguments.
  - *Input File*: Uses tokens for example: `<Info.OutputDirectory0>/<Info.OutputFilename0>` will pick first output folder and first output file of the job using submit info/plugin information.
  - *Input File*: Uses tokens for example: `<Info.OutputDirectory0>/preview.mp4` will pick first output folder of the job using submit info/plugin information.
  - *Job Priority*: ffmpeg job priority.
