<?php
define('DB_USER', 'root');
define('DB_PASSWORD', '');
define('DB_HOST', 'localhost');
define('DB_NAME', 'final');

$dbc = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME);

if ($dbc->connect_error) {
    die("Connection failed: " . $dbc->connect_error);
}
set_time_limit(1000);
$duplicates = [];

//---PART ONE---
$setKeyOne = "
ALTER TABLE `hawkeye_contacts` CHANGE `hc_contact` `hc_contact` INT(4) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (`hc_contact`);
";

$setKeyTwo = "
ALTER TABLE `hawkeye_customer` CHANGE `hc_customer` `hc_customer` INT(4) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (`hc_customer`);
";

$setKeyThree = "
ALTER TABLE `hawkeye_transaction` CHANGE `h_transaction_id` `h_transaction_id` INT(4) NOT NULL AUTO_INCREMENT, add PRIMARY KEY (`h_transaction_id`);
";

$dbc->query($setKeyOne);
$dbc->query($setKeyTwo);
$dbc->query($setKeyThree);

$jewelQuery = "SELECT * FROM jewel_company";
$resultJewel = $dbc->query($jewelQuery);

if ($resultJewel) {
    while ($company = $resultJewel->fetch_assoc()) {
        //get name to check
        $companyName = $company['company_name'];

        //check name
        $checkQuery = $dbc->prepare("SELECT hc_customer FROM hawkeye_customer WHERE hc_name = ?");
        $checkQuery->bind_param("s", $companyName);
        $checkQuery->execute();
        $checkQuery->store_result();

        //if duplicate is found add to $duplicates
        if ($checkQuery->num_rows > 0) {
            $duplicates[] = $companyName;

        //if no duplicate is found add to table
        } else {
            $insertQuery = $dbc->prepare("
                INSERT INTO hawkeye_customer (
                    hc_name, hc_address, hc_city, hc_state, hc_zip, hc_contact
                ) VALUES (?, ?, ?, ?, ?, ?)
            ");
            $insertQuery->bind_param(
                "ssssss",
                $company['company_name'],
                $company['company_address'],
                $company['company_city'],
                $company['company_state'],
                $company['company_zip'],
                $company['company_contact']
            );
            $insertQuery->execute();
            $insertQuery->close();
        }

        $checkQuery->close();
    }
    $resultJewel->free();
} else {
    echo "<p>Error retrieving Jewel companies: " . $dbc->error . "</p>";
}

//---PART TWO---
$moreSql = "
INSERT INTO hawkeye_product (product_line, product_notes)
SELECT jo.product_line, jo.product_notes
FROM jewel_offerings jo
LEFT JOIN hawkeye_product hp ON jo.product_line = hp.product_line
WHERE hp.product_line IS NULL;
";
//join looks for entries already in the hawkeye_product table
//WHERE statement removes any that match
$dbc->query($moreSql);
//---PART THREE---
//Normalized states to abbreviations
$states = [
    'Alabama' => 'AL',
    'Alaska' => 'AK',
    'Arizona' => 'AZ',
    'Arkansas' => 'AR',
    'California' => 'CA',
    'Colorado' => 'CO',
    'Connecticut' => 'CT',
    'Delaware' => 'DE',
    'District of Columbia' => 'DC',
    'Florida' => 'FL',
    'Georgia' => 'GA',
    'Hawaii' => 'HI',
    'Idaho' => 'ID',
    'Illinois' => 'IL',
    'Indiana' => 'IN',
    'Iowa' => 'IA',
    'Kansas' => 'KS',
    'Kentucky' => 'KY',
    'Louisiana' => 'LA',
    'Maine' => 'ME',
    'Maryland' => 'MD',
    'Massachusetts' => 'MA',
    'Michigan' => 'MI',
    'Minnesota' => 'MN',
    'Mississippi' => 'MS',
    'Missouri' => 'MO',
    'Montana' => 'MT',
    'Nebraska' => 'NE',
    'Nevada' => 'NV',
    'New Hampshire' => 'NH',
    'New Jersey' => 'NJ',
    'New Mexico' => 'NM',
    'New York' => 'NY',
    'North Carolina' => 'NC',
    'North Dakota' => 'ND',
    'Ohio' => 'OH',
    'Oklahoma' => 'OK',
    'Oregon' => 'OR',
    'Pennsylvania' => 'PA',
    'Rhode Island' => 'RI',
    'South Carolina' => 'SC',
    'South Dakota' => 'SD',
    'Tennessee' => 'TN',
    'Texas' => 'TX',
    'Utah' => 'UT',
    'Vermont' => 'VT',
    'Virginia' => 'VA',
    'Washington' => 'WA',
    'West Virginia' => 'WV',
    'Wisconsin' => 'WI',
    'Wyoming' => 'WY'
];

$stmt = $dbc->query("SELECT hc_customer, hc_state FROM hawkeye_customer");
while ($row = $stmt->fetch_assoc()) {
    $stateName = $row['hc_state'];
    $customerId = $row['hc_customer'];
    
    if (isset($states[$stateName])) {
        $abbr = $states[$stateName];
        $update = $dbc->prepare("UPDATE hawkeye_customer SET hc_state = ? WHERE hc_customer = ?");
        $update->execute([$abbr, $customerId]);
    }
}

$updateOne = "
    ALTER TABLE hawkeye_customer
    ADD column contact_id INT;
";

$updateTwo = "
    ALTER TABLE hawkeye_customer
    ADD CONSTRAINT fk_contact_id
    FOREIGN KEY (contact_id) REFERENCES hawkeye_contacts(hc_contact);
";

$updateThree = "
    UPDATE hawkeye_customer AS hc
    JOIN hawkeye_contacts AS hc_contact
    ON hc.hc_customer = hc_contact.hc_customer
    SET hc.contact_id = hc_contact.hc_contact;
";

$updateFour = "
    ALTER TABLE hawkeye_customer
    DROP COLUMN hc_contact;
";

$dbc->query($updateOne);
$dbc->query($updateTwo);
$dbc->query($updateThree);
$dbc->query($updateFour);

//---PART FOUR---
$sqlThing = "
    INSERT INTO hawkeye_contacts (hc_fname, hc_lname, hc_phone, hc_birthday, hc_customer)
    SELECT
    SUBSTRING_INDEX(jc.company_contact, ' ', 1) AS hc_fname,
    SUBSTRING_INDEX(jc.company_contact, ' ', -1) AS hc_lname,
        jc.company_contact_phone AS hc_phone,
        jc.company_customer_account_opened AS hc_birthday,
        hcu.hc_customer
    FROM jewel_company jc
    JOIN hawkeye_customer hcu
        ON jc.company_name = hcu.hc_name
    ON DUPLICATE KEY UPDATE
        hc_fname = VALUES(hc_fname),
        hc_lname = VALUES(hc_lname),
        hc_phone = VALUES(hc_phone),
        hc_birthday = VALUES(hc_birthday),
        hc_customer = VALUES(hc_customer);
";

$sqlUpdate = "
    UPDATE hawkeye_customer AS hc
    JOIN hawkeye_contacts AS hc_contact
        ON hc.hc_customer = hc_contact.hc_customer
    SET hc.contact_id = hc_contact.hc_contact;
";
$dbc->query($sqlThing);
$dbc->query($sqlUpdate);

//---PART FIVE---
$sqlTrans = "
    INSERT INTO hawkeye_transaction (h_product_id, h_customer_id, h_purchase_date, h_amt)
    SELECT
        hp.product_id AS h_product_id,
        hc.hc_customer AS h_customer_id,
        jt.purchase_date AS h_purchase_date,
        jt.Amt AS h_amt
    FROM jewel_transactions jt
    JOIN hawkeye_product hp 
        ON jt.product_line = hp.product_line
    JOIN jewel_company jc 
        ON jt.customer_id = jc.company_code
    JOIN hawkeye_customer hc 
        ON jc.company_name = hc.hc_name;
";
$dbc->query($sqlTrans);
$dbc->close();

echo "<h2>Import complete.</h2>";
if (!empty($duplicates)) {
    echo "<h3>Duplicates not imported:</h3><ul>";
    foreach ($duplicates as $name) {
        echo "<li>" . htmlspecialchars($name) . "</li>";
    }
    echo "</ul>";
} else {
    echo "<p>No duplicates found.</p>";
}
?>