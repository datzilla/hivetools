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
$escapesep = false;
$minimum_col = 1;
$dropexists = false;
$withquotes = false;
$columnprefix = 'V';
$temp_file = fopen ("tmp.hql", "w");

$usage_msg = '--pf : parent folder containing the text or gz files on hdfs. Do not add trailing /' . PHP_EOL;
$usage_msg .= '--db : the name of the database in hive, if not specified, it will use "default"' . PHP_EOL;
$usage_msg .= '--tb : the name of the table in hive database. If not specified, the script will not run.' . PHP_EOL;
$usage_msg .= '--drop : drops the table before creating.' . PHP_EOL;
$usage_msg .= '--sep : the column separator, for tab you do not need to specify \\ before the t. Just simple t. Same for pipe characters' . PHP_EOL;
$usage_msg .= '--escapesep : if the seperator is | or ", the character needs to be escaped. this switch is then used.' . PHP_EOL;
$usage_msg .= '--withheader : treats the first line of the text dataset as the header row. default is off' . PHP_EOL;
$usage_msg .= '--withquotes : treats double quotes as a string value wrapper. default is off' . PHP_EOL;
$usage_msg .= '--mincol : specifies the minimum number of columns in the text dataset. Script does not run if it is not met. Default is 3' . PHP_EOL;
$usage_msg .= '--columnprefix : this will only work when there\'s no column header specified' . PHP_EOL;
$usage_msg .= 'Example: php create_hive_table_single.php --db "dats_experiment" --tb "fixed_dart" --pf "/user/dat.nguyen/fixed_dart" --sep "," --withheader --withquotes --drop --mincol 3' . PHP_EOL;
$usage_msg .= 'Alternative: php create_hive_table_single.php --db "dats_experiment" --tb "fixed_dart" --pf "/user/dat.nguyen/fixed_dart" --sep "," --columnprefix "FDC" --withquotes --drop --mincol 3' . PHP_EOL;

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

	if ($argv[$i]== '--colprefix'){
	   $columnprefix = $argv[$i + 1];
	}

	if ($argv[$i] == '--escapesep') {
	   $escapesep = true;
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
	    $newsep = "";
	    if ($sep=="t") {
		 $newsep = "\t";
	    }elseif ($sep=="n") {
		$newsep = "\n";
	    } else {
		$newsep = $sep;
	    }
            $columns = explode ($newsep, $line);
            $total_columns = count ($columns);
	    if ($total_columns >= $minimum_col){
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

                        // if there's _ at the front and the end of the string, just remove it and also tidy up when there's more than __.
                        $columns [$k] = "" . str_replace('__', '_', $columns [$k]);
                        $columns [$k] = ltrim ($columns[$k], '_');
                        $columns [$k] = rtrim ($columns[$k], '_');

                        $columns [$k] = '`'. $columns[$k]  . "` STRING";
                    }
                } else {
                    for ($k = 0; $k < $total_columns; $k++) { 
                        $columns [$k] = $columnprefix . $k . " STRING";
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
            $hql .= ' "separatorChar" = "';
	    if ($escapesep == TRUE) $hql .= '\\';
	    $hql .= $sep . '"';
            if ($withquotes == TRUE) $hql .= ', "quoteChar" = "\\""';
            $hql .= ' ) ';
            $hql .= "STORED AS TEXTFILE ";
            $hql .= "LOCATION '" . $foldername . "'";
            if ($withheader == TRUE) $hql .= ' tblproperties("skip.header.line.count"="1")';
            $hql .= ";";

            fwrite ($temp_file, $hql);
	        //echo $hql . PHP_EOL;
	    try {
            	shell_exec ('hive -f "tmp.hql"');
	    } catch (Exception $e) {
		echo $e . PHP_EOL;
	    }
            fclose ($temp_file);

        } else {
            
            echo "something is wrong here? column count is incorrect when trying to retrieve from data file" . PHP_EOL;

        }

    } else {

        echo "something is wrong here? cannot read the data file" . PHP_EOL;

    }
} else {
    echo "something is wrong here? insufficient inputs from command parameters" . PHP_EOL;
    echo $usage_msg;
}


?>


