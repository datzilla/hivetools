<?php

/*

    This script creates a  hive table from a single external text file on dfs.
    
*/

//require_once ("../functions.php");

$foldername = null;
$sep = null;
$dbname = 'default';
$tbname = null;
$withheader = false;
$minimum_col = 3;
$dropexists = false;
$withquotes = false;
$temp_file = fopen ("tmp.hql", "w");

$usage_msg = '--pf : parent folder containing the text or gz files on hdfs. Do not add trailing /' . PHP_EOL;
$usage_msg .= '--db : the name of the database in hive, if not specified, it will use "default"' . PHP_EOL;
$usage_msg .= '--tb : the name of the table in hive database. If not specified, the script will not run.' . PHP_EOL;
$usage_msg .= '--drop : drops the table before creating.' . PHP_EOL;
$usage_msg .= '--sep : the column separator, for tab you do not need to specify \\ before the t. Just simple t. Same for pipe characters' . PHP_EOL;
$usage_msg .= '--withheader : treats the first line of the text dataset as the header row. default is off' . PHP_EOL;
$usage_msg .= '--withquotes : treats double quotes as a string value wrapper. default is off' . PHP_EOL;
$usage_msg .= '--mincol : specifies the minimum number of columns in the text dataset. Script does not run if it is not met. Default is 3' . PHP_EOL;
$usage_msg .= 'Example: php create_hive_table_single.php --db "dats_experiment" --tb "fixed_dart" --pf "/user/dat.nguyen/fixed_dart" --sep "," --withheader --withquotes --drop --mincol 3' . PHP_EOL;

if (count ($argv) > 1){
    for ($i = 0; $i < count ($argv); $i++) {
        if ($argv[$i] == '--db') {
            if (isset ($argv[$i+1])) {
                $dbname = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--tb') {
            if (isset ($argv[$i+1])) {
                $tbname = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--pf') {
            if (isset ($argv[$i+1])) {
                $foldername = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--sep') {
            if (isset ($argv[$i+1])) {
                $sep = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--withheader') {
                $withheader = true;
        }

        if ($argv[$i] == '--mincol') {
            if (isset ($argv[$i+1])) {
                $minimum_col = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--withquotes') {
            $withquotes = true;
        }

        if ($argv[$i] == '--drop') {
            $dropexists = true;
        }

    }
}

if (
    !empty ($foldername) &&
    !empty ($sep) &&
    !empty ($dbname) &&
    !empty ($tbname)
    ) {

    $exec_out = shell_exec ('hdfs dfs -text "' . $foldername . '/*" | head -n 1');
    $outlines = explode ("\n", $exec_out);

    if (isset ($outlines[0])) {

        $attribute_str = null;
        foreach ($outlines as $line) {

            $columns = explode ($sep, $line);
            $total_columns = count ($columns);
            
            if ($total_columns > $minimum_col){
                // let's create the column headers with the first rows name
                if ($withheader == true) {
                    for ($k = 0; $k < $total_columns; $k++) {
                        $columns [$k] = trim ($columns [$k]);
                        // we need to remove any characters that would be an illegal name
                        
                        // if the first character and the end character is quote, we need to remove it. 
                        $columns [$k] = ltrim ($columns[$k], '"');
                        $columns [$k] = rtrim ($columns[$k], '"');
                        $columns [$k] = "" . str_replace('-', '_', $columns [$k]);
                        $columns [$k] = "" . str_replace('(', '_', $columns [$k]);
                        $columns [$k] = "" . str_replace(')', '_', $columns [$k]);
                        $columns [$k] = "" . str_replace('.', '_', $columns [$k]);
                        $columns [$k] = "" . str_replace('/', '_', $columns [$k]);
                        $columns [$k] = "" . str_replace('\\', '_', $columns [$k]);
                        $columns [$k] = "" . str_replace(' ', '_', $columns [$k]);

                        $columns [$k] = 'f'. $columns[$k]  . " STRING";
                    }
                } else {
                    for ($k = 0; $k < $total_columns; $k++) { 
                        $columns [$k] = "V" . $k . " STRING";
                    }
                }

                $attribute_str = implode (", ", $columns);
	            //	echo   $attribute_str . PHP_EOL;
            }
        }

        if (!is_null ($attribute_str)) {

            // drop if exists
            if ($dropexists == TRUE) {
                shell_exec ('hive -e "DROP TABLE IF EXISTS ' . $dbname . '.' . $tbname . '";');
            }

            // execute the query
            $hql = "CREATE EXTERNAL TABLE " . $dbname . "." . $tbname . " (";
            $hql .= $attribute_str;
            $hql .= ") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ";
            $hql .= "WITH SERDEPROPERTIES ( ";
            $hql .= ' "separator" = "\\' . $sep . '", ';
            if ($withquotes == TRUE) $hql .= ' "quoteChar" = "\\""';
            $hql .= ' ) ';
            $hql .= "STORED AS TEXTFILE ";
            $hql .= "LOCATION '" . $foldername . "'";
            if ($withheader == TRUE) $hql .= ' tblproperties("skip.header.line.count"="1")';
            $hql .= ";";

            fwrite ($temp_file, $hql);
	        //echo $hql . PHP_EOL;
            shell_exec ('hive -f "tmp.hql"');
            fclose ($temp_file);

        } else {
            
            echo "something is wrong here? could not form attribute strings" . PHP_EOL;

        }

    } else {

        echo "something is wrong here? cannot read the text file" . PHP_EOL;

    }
} else {
    echo "something is wrong here? insufficient input parameters" . PHP_EOL;
    echo $usage_msg;
}


?>

