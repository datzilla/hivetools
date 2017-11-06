<?php

/*

    This script creates the hive table from text files that are stored under within multiple sub directories.
    These subdirectories are usually partitioned by date or an incremental string or integer.
    
*/

//require_once ("../functions.php");

$pfolder = null;
$sep = null;
$dbname = null;
$tbname = null;
$subfolders = null;
$column_count = null;

$temp_hql_file = fopen ("tmp_create_file.hql", "w");

if (count ($argv) > 0){
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
                $pfolder = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--sep') {
            if (isset ($argv[$i+1])) {
                $sep = $argv[$i+1];
            }
        }
    }
} else {

    echo 'Example: php create_hive_table.php --db "dats_experiment" --tb "custprofiletier0" --pf "/tier0/custprofile" --sep "|"' . PHP_EOL;
    
}



if (
    !empty ($pfolder) &&
    !empty ($sep) &&
    !empty ($dbname) &&
    !empty ($tbname)
    ) {

    $exec_out = shell_exec ('hdfs dfs -ls -d "' . $pfolder . '/*/"');
    $outlines = explode ("\n", $exec_out);

    if (isset ($outlines[0])) {

        foreach ($outlines as $line) {

            preg_match ('/.*?\/([\w\d\-]+)$/', $line, $matches);

            if (count ($matches) > 1) {

                if (is_null ($subfolders)) $subfolders = array ();
                array_push ($subfolders, $matches[1]);

            }

            $matches = null;
        }

        // If there're subfolders then
        $subfolder_count = count ($subfolders);
        if ($subfolder_count > 0 ){

            // get the column count
            $exec_out = shell_exec ('hdfs dfs -text "' . $pfolder . '/' . $subfolders[0] . '" | head -n 1');
            $outlines = explode ("\n", $exec_out);
        
            if (isset ($outlines[0])) {
                $columns = explode ($sep, $outlines [0]);
                if  (!empty ($columns)) {
                    $column_count = count ($columns);
                } else {
                    echo "something is wrong here? can't read the column headers" . PHP_EOL;        
                }
            }

            echo 'columns: ' . $column_count . PHP_EOL;

            /*
            for ($k = 0; $k < $subfolder_count; $k++) {



            }*/

        } else {
            // it's not a column
        }

    } else {

        echo "something is wrong here? can't read the text file" . PHP_EOL;

    }
} else {
    echo "something is wrong here? there's no pfolder and separator" . PHP_EOL;
}




?>