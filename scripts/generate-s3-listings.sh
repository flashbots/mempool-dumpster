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
    .header {
      margin: 4em;
      margin-bottom: 2em;
    }
    .content { padding: 2em 4em; }
    td.fn { padding-right: 120px; }
    td.fs { text-align: right; }
    tr:hover { background: #f7faff; }
  </style>
</head>
<body>
  <div class="header">
    <a href="https://collective.flashbots.net/">
      <img style="float:right; background:white; margin-left: 64px; width: 100px; height: 100px;" src="https://d33wubrfki0l68.cloudfront.net/ae8530415158fbbbbe17fb033855452f792606c7/fe19f/img/logo.png">
    </a>
    <h1>Mempool Dumpster</h1>
    <p>
      <a href="https://github.com/flashbots/mempool-dumpster">https://github.com/flashbots/mempool-dumpster</a>
    </p>
  </div>
  <div class="content">
END
)

HTML_FOOTER="</div></body></html>"


# Cleanup output directory
outdir="/tmp/s3-index/ethereum/mainnet"
rm -rf $outdir
mkdir -p $outdir

# Start writing local root index file
fn_root_index="${outdir}/index.html"
echo "writing root index at $fn_root_index"
echo $HTML_HEADER > $fn_root_index
echo "<ul>" >> $fn_root_index

# Iterate over all months in S3
dirs=$( aws s3 ls s3://flashbots-mempool-dumpster/ethereum/mainnet/ --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com" | awk '{ print $2 }' )
for dir in $dirs; do
    # Add entry to root index.html
    echo "<li><a href=\"/ethereum/mainnet/${dir}index.html\">${dir}</a></li>" >> $fn_root_index

    # create subdirectory and write index.html
    subdir="${outdir}/${dir}"
    mkdir -p $subdir

    # Start writing local subdirectory file listing html
    fn_files_listing="${subdir}index.html"
    echo "writing $fn_files_listing ..."
    echo $HTML_HEADER > $fn_files_listing
    # echo "<p><a href=\"/index.html\">back</a><p>" >> $fn_files_listing
    echo "<table class=\"pure-table pure-table-horizontal\"><tbody>" >> $fn_files_listing
    echo "<tr><td class="fn"><a href="/index.html">..</a></td><td></td></tr>" >> $fn_files_listing
    # echo "<thead><tr><th>File</th><th>Size</th></tr></thead>" >> $fn_files_listing

    # Iterate over all files in this S3 directory and add them to file listing html
    aws s3 ls s3://flashbots-mempool-dumpster/ethereum/mainnet/2023-08/ --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com" | while read line; do
        file_date=$( echo $line | awk '{ print $1" "$2 }' )
        file_size=$( echo $line | cut -d' ' -f3 | numfmt --to=iec-i --suffix=B )
        file_name=$( echo $line | cut -d' ' -f4 )
        if [ "$file_name" != "index.html" ]; then
          echo "<tr><td class=\"fn\"> <a href=\"/ethereum/mainnet/${dir}${file_name}\">${file_name}</a> </td><td class='fs'> ${file_size} </td></tr>" >> $fn_files_listing
        fi
    done

    # Close and upload local file listing html
    echo "</tbody></table> $HTML_FOOTER" >> $fn_files_listing
    echo "uploading ${dir}index.html to S3..."
    aws s3 cp $fn_files_listing "s3://flashbots-mempool-dumpster/ethereum/mainnet/${dir}" --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
done

# Close and upload local root index.html
echo "</ul> $HTML_FOOTER" >> $fn_root_index
echo "uploading root index to S3..."
aws s3 cp /tmp/s3-index/ethereum/mainnet/index.html s3://flashbots-mempool-dumpster/ --endpoint-url "https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
