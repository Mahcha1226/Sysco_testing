source ./lms_Log.sh

source ./lms_Log.sh1


logLMS "Commencing LMS Download Step "
TOTALOBJECTS=$(aws s3 ls  s3://sysco-dev-odi-us-east-1/LMSOutboundRP/ --recursive --profile=odi-go-dev | wc -l)
echo "$TOTALOBJECTS"
OBJECTS=$(aws s3 ls s3://sysco-dev-odi-us-east-1/LMSOutboundRP/ --profile=odi-go-dev  | awk '{print $4}')
COUNT=$(expr $TOTALOBJECTS - 1)
echo "$COUNT"
echo "$OBJECTS"
logLMS "File Count in S3 bucket is -  $COUNT"
logLMS "Items in S3 Bucket ${OBJECTS[@]}"

logLMS "Starting Download from S3"

aws s3 cp s3://sysco-dev-odi-us-east-1/LMSOutboundRP/ /efs-share/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP --recursive --profile=odi-go-dev
SFTP_RESPONSE=${?}
logLMS "Response From S3 - $SFTP_RESPONSE"
if [[ 0 != ${SFTP_RESPONSE} ]]
then
    logLMS "Downloading files/file Failed!"

else
    logLMS "Downloading files/file Successful!"
fi

FILES=$(ls /efs-share/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP/* )
NUMOFFILES=$(ls /efs-share/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP/* | wc -l)


logLMS "Verifying files downloaded from S3"

if [ "$NUMOFFILES" -gt 0 ]
then
    logLMS "$NUMOFFILES - files downloaded from S3"
    for FILE in $FILES
        do
        logLMS "$FILE was downloaded !"
    done

else
    logLMS "No Files were downloaded !"    
fi

if [ -z "$(ls -A /efs-share/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP)" ]; then
    logLMS "Empty Directory "
else
    DESTFILES="/efs-share/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP/*"
    for eachfile in $DESTFILES; do
        filesize=$(stat --format=%s "${eachfile}")
        logLMS "Current File Full Path: - ${eachfile}, File Size: - ${filesize} "
        FILENAME=${eachfile##*/}
        if (( $filesize == 0 )); then
            rm $eachfile
            logLMS "Deleted ${filesize} bytes file ${FILENAME}"
        fi
    done
fi
