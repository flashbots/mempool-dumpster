#!/bin/bash
#
# Generate and upload index.html for files in S3
#
set -o errexit
set -o nounset
set -o pipefail

# change to project root directory and load environment variables
cd "$(dirname "$0")"
cd ..
source .env.prod

HTML_HEADER=$(cat <<-END
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/purecss@3.0.0/build/pure-min.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/purecss@3.0.0/build/grids-min.css">
  <style>
    .content { padding: 2em 4em; }
    td.fn { padding-right: 120px; }
    td.fs { text-align: right; }
    tr:hover { background: #f7faff; }
  </style>
</head>
<body>
  <div class="content">
END
)

# remove and create output directory
outdir="/tmp/s3-index/ethereum/mainnet"
rm -rf $outdir
mkdir -p $outdir

# start writing index file
fn="${outdir}/index.html"
echo "writing $fn ..."
echo $HTML_HEADER > $fn
echo "<ul>" >> $fn

# iterate over all directories
DIRS=$( aws s3 ls s3://flashbots-mempool-dumpster/ethereum/mainnet/ --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com" | awk '{ print $2 }' )
for DIR in $DIRS; do
    echo "<li><a href=\"/ethereum/mainnet/${DIR}index.html\">${DIR}</a></li>" >> $fn

    subdir="${outdir}/${DIR}"
    mkdir -p $subdir
    fn_listing="${subdir}index.html"
    echo "writing $fn_listing ..."
    echo $HTML_HEADER > $fn_listing
    echo "<p><a href=\"/index.html\">back</a><p>" >> $fn_listing
    echo "<table class=\"pure-table pure-table-horizontal\"><tbody>" >> $fn_listing
    # echo "<thead><tr><th>File</th><th>Size</th></tr></thead>" >> $fn_listing

    aws s3 ls s3://flashbots-mempool-dumpster/ethereum/mainnet/2023-08/ --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com" | while read line; do
        file_date=$( echo $line | awk '{ print $1" "$2 }' )
        file_size=$( echo $line | cut -d' ' -f3 | numfmt --to=iec-i )
        file_name=$( echo $line | cut -d' ' -f4 )
        if [ "$file_name" != "index.html" ]; then
          echo "<tr><td class=\"fn\"> <a href=\"/ethereum/mainnet/${DIR}${file_name}\">${file_name}</a> </td><td class='fs'> ${file_size} </td></tr>" >> $fn_listing
        fi
    done
    echo "</tbody></table></div></body></html>" >> $fn_listing

    echo "uploading to S3..."
    aws s3 cp $fn_listing "s3://flashbots-mempool-dumpster/ethereum/mainnet/${DIR}" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

done
echo "</ul></div></body></html>" >> $fn

echo "uploading root index to S3..."
aws s3 cp /tmp/s3-index/ethereum/mainnet/index.html s3://flashbots-mempool-dumpster/ --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
