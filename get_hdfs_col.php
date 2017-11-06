<?php

$inputfile = null;
$sep = null;

if (count ($argv) > 0){
    for ($i = 0; $i < count ($argv); $i++) {
        if ($argv[$i] == '--file') {
            if (isset ($argv[$i+1])) {
                $inputfile = $argv[$i+1];
            }
        }

        if ($argv[$i] == '--sep') {
            if (isset ($argv[$i+1])) {
                $sep = $argv[$i+1];
            }
        }
    }
} else {

    echo '--file "filename.csv"' . ' --sep "separator"' . PHP_EOL;
    
}


if (!empty($inputfile) && !empty ($sep)) {
    $exec_out = shell_exec ('hdfs dfs -text "' . $inputfile . '" | head -n 1');
    $outlines = explode ("\n", $exec_out);

    if (isset ($outlines[0])) {
        $columns = explode ($sep, $outlines [0]);
        if (count ($columns)> 1) {
//	echo count ($columns) . PHP_EOL;
            foreach ($columns as $index => $name) {
                echo "idx: " . $index . " name: " . $name . PHP_EOL;
            }
        } else {
//            echo "something is wrong here? can't read the column headers" . PHP_EOL;        
        }
    } else {
	echo "something is wrong here? can't read the text file on hdfs" . PHP_EOL;
    }
} else {
    echo "something is wrong here? there's no inputfile and separator" . PHP_EOL;
}

?>
