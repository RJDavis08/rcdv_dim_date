with 
/*
    STG COUNTRY CODE

*/
source as (

    select * from {{ ref("country_codes") }}

),

renamed as (

    select * from source

)

select * from renamed