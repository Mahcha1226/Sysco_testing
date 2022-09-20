. /efs-share/INFORMATICA/infa_shared/Scripts/ODI/prod.profile

cd /efs-share/INFORMATICA/infa_shared/Scripts/ODI


###Load data from dw.fact_dailydeliverytrips_rt to dm.lms_actual_rt
sh masterscript_rp.sh usp_LoadDMcreateactualrtLMS_RP usp_LoadDMcreateactualrtLMS_RP.sql
if [ $? -ne 0 ]
then
  exit 1
fi

###Load data fron dm.lm_actual_rt to dm.lms_actual_outbound_type1
sh masterscript_rp.sh usp_LoadDMActualoutboundLMS_type1_RP usp_LoadDMActualoutboundLMS_type1_RP.sql
if [ $? -ne 0 ]
then
  exit 1
fi

###Unload OPCOs data to export to BY
python UnloadExportByOPCOSQLLMS_RP.py LMSActualsUnload_RP.sql
if [ $? -ne 0 ]
then
  exit 1
fi

###Download the RS unloaded OPCO data file to Unix
###Source s3://sysco-dev-odi-us-east-1/LMSOutboundRP/
###Target /efs-share/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP
sh lms_download_rp.sh
if [ $? -ne 0 ]
then
  exit 1
fi

###Transfer the downloaded OPCO file to BY via BIS
####Source file path /AppScripts/INFORMATICA/infa_shared/BWParam/ODI/LMS/RP
####Failed file folder /AppScripts/INFORMATICA/infa_shared/BWParam/ODI/LMS_FAILED/RP
bash lms_sftp_split_rp.sh
