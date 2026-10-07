# backupr
This is a python script to interact with flickr's API to download all of your own images for backup purposes.

## Running
You will need to get a (non-commercial) flickr API key at [https://www.flickr.com/services/apps/create/](https://www.flickr.com/services/apps/create/).
Then you will need to export the secrets as environment variables for the script.
### `uv`
```bash
export FLICKR_API_KEY=xxxx
export FLICKR_API_SECRET=yyyy
uv run main.py [backup directory path]
```
### `pip`
```bash
export FLICKR_API_KEY=xxxx
export FLICKR_API_SECRET=yyyy
pip install requests requests-oauthlib tqdm
python flickr_album_downloader.py [backup directory path]
```

## Disclaimer
This script and it's authors are not affiliated with flickr. Use of this script is at your own risk.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
