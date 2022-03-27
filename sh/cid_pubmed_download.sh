#/bin/bash
#. ../config/mysql_conf.sh
export HOME2=`pwd`
export PROCESS="get_cid_pubmend_json"
export DATA="$HOME2/data"
export LOG="$HOME2/log"
export dt=`date +"%Y-%m-%d_%H_%M_%S"`
export LOG_FILE="$LOG/$PROCESS_$dt.txt"
export mysql_server="23.23.4.75"
export table_name="cid_process"
repeat_num=$1





 echo "start process at `date +'%Y-%m-%d_%H:%M:%S'`" > $LOG_FILE


if ! [ $repeat_num ]; then
  echo "please input number of times to run"
else
  if echo $repeat_num | egrep -q '^[0-9]+$' ; then
    
    # $var is a number
    for (( k=0; k < $repeat_num; k++ )); do
        if ! [ $2 ]; then
            echo "no number of item per run defined use default 10"
            process_num="10"
        else 
            if echo $2 | egrep -q '^[0-9]+$' ; then
                echo "number of item per run defined as ${2}"
                process_num=$2
            else
                echo "number of item per run defined incorrect use default 10"
                process_num="10"
            fi    
        fi
 
        #####get cids to process
        mysql -h $mysql_server -D pubmed_stg --skip-column-names -e "select cid from ${table_name} where assigned=0 and retrieved=0 order by rand() limit ${process_num};" > $DATA/cid.txt
        IFS=$'\n'       # make newlines the only separator
        set -f          # disable globbing
        assign_stmt="update ${table_name} set assigned=1 where cid in (0"
        for j in $(cat $DATA/cid.txt); do 
            assign_stmt="${assign_stmt},$j"
        done
        assign_stmt="${assign_stmt});"
        echo $assign_stmt
        mysql -h $mysql_server -D pubmed_stg --skip-column-names -e "$assign_stmt"
        for i in $(cat $DATA/cid.txt); do
          #echo "tester: $i"
            CID=$i
        
        
            export FILENAME="CID_$CID.json"
            
            export URL="https://pubchem.ncbi.nlm.nih.gov/sdq/sdqagent.cgi?infmt=json&outfmt=json&query={%22download%22:%22*%22,%22collection%22:%22pubmed%22,%22where%22:{%22ands%22:[{%22cid%22:%22$CID%22},{%22pmidsrcs%22:%22xref%22}]},%22order%22:[%22articlepubdate,desc%22],%22start%22:1,%22limit%22:10000000,%22downloadfilename%22:%22CID_$CID%22}"
            
            wget -O $DATA/$FILENAME $URL >> $LOG_FILE
            sleep 5
            aws s3 cp $DATA/$FILENAME s3://jk-data-files/pubmed_cid/raw/
            mysql -h $mysql_server -D pubmed_stg --skip-column-names -e "update ${table_name} set retrieved=1 where cid=${CID};"
            mysql -h $mysql_server -D pubmed_stg --skip-column-names -e "insert into cid_pubmed_json_raw (json_string) values ('`cat ${DATA}/${FILENAME}`');"
        done
        find $DATA -type f -name "*.json" -exec rm -f {} \;
   done
  else
    # $var is not a number
    echo "please input a number"
  fi
fi