class Sql:
    blended_data_select = """
        SELECT 
            guid,
            datetime,
            sender,
            recipients,
            subject,
            attachments,
            rule,
            action,
            attach_cnt,
            rcpt_cnt,
            email_size,
            log_file,
            log_key_id,
            name,
            phone_number,
            address
        FROM blended_email_logs
        """
    blended_data_headers = """guid
datetime
sender
recipients
subject
attachments
rule
action
attach_cnt
rcpt_cnt
email_size
log_file
log_key_id
name
phone_number
address""".split()
    
    blended_data_insert = ("""
        INSERT INTO blended_email_logs(
                            guid,
                            datetime,
                            sender,
                            recipients,
                            subject,
                            attachments,
                            rule,
                            action,
                            attach_cnt,
                            rcpt_cnt,
                            email_size,
                            log_file,
                            log_key_id,
                            name,
                            phone_number,
                            address
        )
        SELECT 
             el.guid, 
             el.datetime,
             el.sender,
             el.recipients,
             el.subject,
             el.attachments,
             el.rule,
             el.action,
             el.attach_cnt,
             el.rcpt_cnt,
             el.email_size,
             el.log_file,
             el.log_key_id,
             e.name,
             e.phone_number,
             e.address
        FROM email_logs el
        LEFT JOIN employees e ON el.sender = e.email
        """)

    blended_data_table_create = """CREATE TABLE IF NOT EXISTS blended_email_logs (
                guid varchar, 
                "datetime" TIMESTAMP, 
                sender varchar, 
                recipients varchar, 
                subject varchar, 
                attachments varchar, 
                rule varchar, 
                action varchar, 
                attach_cnt int, 
                rcpt_cnt int, 
                email_size int, 
                log_file varchar, 
                log_key_id varchar, 
                name varchar,
                phone_number varchar,
                address varchar,
                PRIMARY KEY(guid, sender))"""
    
    employees_table_drop = """DROP TABLE IF EXISTS employees"""
    
    employees_table_create = """CREATE TABLE employees (
                    name varchar,
                    email varchar, 
                    phone_number varchar,
                    address varchar,
                    PRIMARY KEY (name, email));"""
    
    employees_csv_header_order = """name,email,phone_number,address"""
        
    email_log_table_create = """CREATE TABLE IF NOT EXISTS email_logs 
            (guid varchar, 
            "datetime" TIMESTAMP, 
            sender varchar, 
            recipients varchar, 
            subject varchar, 
            attachments varchar, 
            rule varchar, 
            action varchar, 
            attach_cnt int, 
            rcpt_cnt int, 
            email_size int, 
            log_file varchar, 
            log_key_id varchar,
            PRIMARY KEY (guid, sender));"""
    
    email_log_csv_header_order = """guid,datetime,sender,recipients,subject,attachments,rule,action,attach_cnt,rcpt_cnt,email_size,log_file,log_key_id"""