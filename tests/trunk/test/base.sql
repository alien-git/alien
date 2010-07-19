--
-- Selected TOC Entries:
--
--
-- TOC Entry ID 2 (OID 2912697)
--
-- Name: g_object Type: TABLE Owner: subho
--

CREATE TABLE g_object (
	g_name character varying(255) NOT NULL,
	g_association character varying(5) DEFAULT 'False',
	g_parent character varying(255),
	g_child character varying(255),
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 3 (OID 2912737)
--
-- Name: g_object_log Type: TABLE Owner: subho
--

CREATE TABLE g_object_log (
	g_name character varying(255) NOT NULL,
	g_association character varying(5) DEFAULT 'False',
	g_parent character varying(255),
	g_child character varying(255),
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 4 (OID 2912777)
--
-- Name: g_attribute Type: TABLE Owner: subho
--

CREATE TABLE g_attribute (
	g_object character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_data_type character varying(255),
	g_primary_key character varying(5) DEFAULT 'False',
	g_required character varying(5) DEFAULT 'False',
	g_fixed character varying(5) DEFAULT 'False',
	g_values character varying(255),
	g_default_value character varying(255),
	g_sequence integer,
	g_hidden character varying(5) DEFAULT 'False',
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 5 (OID 2912824)
--
-- Name: g_attribute_log Type: TABLE Owner: subho
--

CREATE TABLE g_attribute_log (
	g_object character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_data_type character varying(255),
	g_primary_key character varying(5) DEFAULT 'False',
	g_required character varying(5) DEFAULT 'False',
	g_fixed character varying(5) DEFAULT 'False',
	g_values character varying(255),
	g_default_value character varying(255),
	g_sequence integer,
	g_hidden character varying(5) DEFAULT 'False',
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 6 (OID 2912871)
--
-- Name: g_action Type: TABLE Owner: subho
--

CREATE TABLE g_action (
	g_object character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_display character varying(5) DEFAULT 'False',
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 7 (OID 2912906)
--
-- Name: g_action_log Type: TABLE Owner: subho
--

CREATE TABLE g_action_log (
	g_object character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_display character varying(5) DEFAULT 'False',
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 8 (OID 2912941)
--
-- Name: g_transaction Type: TABLE Owner: subho
--

CREATE TABLE g_transaction (
	g_id integer NOT NULL,
	g_object character varying(255) NOT NULL,
	g_action character varying(255) NOT NULL,
	g_actor character varying(255) NOT NULL,
	g_name character varying(255),
	g_child character varying(255),
	g_count integer,
	g_details character varying(255),
	g_description character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 9 (OID 2912980)
--
-- Name: g_transaction_log Type: TABLE Owner: subho
--

CREATE TABLE g_transaction_log (
	g_id integer NOT NULL,
	g_object character varying(255) NOT NULL,
	g_action character varying(255) NOT NULL,
	g_actor character varying(255) NOT NULL,
	g_name character varying(255),
	g_child character varying(255),
	g_count integer,
	g_details character varying(255),
	g_description character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 10 (OID 2913019)
--
-- Name: g_system Type: TABLE Owner: subho
--

CREATE TABLE g_system (
	g_name character varying(255) NOT NULL,
	g_version character varying(255) NOT NULL,
	g_description character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 11 (OID 2913052)
--
-- Name: g_system_log Type: TABLE Owner: subho
--

CREATE TABLE g_system_log (
	g_name character varying(255) NOT NULL,
	g_version character varying(255) NOT NULL,
	g_description character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 12 (OID 2913085)
--
-- Name: g_user Type: TABLE Owner: subho
--

CREATE TABLE g_user (
	g_name character varying(255) NOT NULL,
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 13 (OID 2913119)
--
-- Name: g_user_log Type: TABLE Owner: subho
--

CREATE TABLE g_user_log (
	g_name character varying(255) NOT NULL,
	g_description character varying(255),
	g_special character varying(5) DEFAULT 'False',
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 14 (OID 2913153)
--
-- Name: g_role Type: TABLE Owner: subho
--

CREATE TABLE g_role (
	g_name character varying(255) NOT NULL,
	g_description character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 15 (OID 2913185)
--
-- Name: g_role_log Type: TABLE Owner: subho
--

CREATE TABLE g_role_log (
	g_name character varying(255) NOT NULL,
	g_description character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 16 (OID 2913217)
--
-- Name: g_role_action Type: TABLE Owner: subho
--

CREATE TABLE g_role_action (
	g_role character varying(255) NOT NULL,
	g_object character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_instance character varying(255) NOT NULL,
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 17 (OID 2913251)
--
-- Name: g_role_action_log Type: TABLE Owner: subho
--

CREATE TABLE g_role_action_log (
	g_role character varying(255) NOT NULL,
	g_object character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_instance character varying(255) NOT NULL,
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 18 (OID 2913285)
--
-- Name: g_role_user Type: TABLE Owner: subho
--

CREATE TABLE g_role_user (
	g_role character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 19 (OID 2913317)
--
-- Name: g_role_user_log Type: TABLE Owner: subho
--

CREATE TABLE g_role_user_log (
	g_role character varying(255) NOT NULL,
	g_name character varying(255) NOT NULL,
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 20 (OID 2913349)
--
-- Name: g_password Type: TABLE Owner: subho
--

CREATE TABLE g_password (
	g_user character varying(255) NOT NULL,
	g_password character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 21 (OID 2913381)
--
-- Name: g_password_log Type: TABLE Owner: subho
--

CREATE TABLE g_password_log (
	g_user character varying(255) NOT NULL,
	g_password character varying(255),
	g_deleted character varying(5) DEFAULT 'False',
	g_creation_time integer NOT NULL,
	g_modification_time integer NOT NULL,
	g_request_id integer NOT NULL,
	g_transaction_id integer NOT NULL
);

--
-- TOC Entry ID 22 (OID 2913413)
--
-- Name: g_key_generator Type: TABLE Owner: subho
--

CREATE TABLE g_key_generator (
	g_name character varying(255) NOT NULL,
	g_next_id integer NOT NULL
);

--
-- TOC Entry ID 23 (OID 2913424)
--
-- Name: g_undo Type: TABLE Owner: subho
--

CREATE TABLE g_undo (
	g_request_id integer NOT NULL
);

--
-- Data for TOC Entry ID 24 (OID 2912697)
--
-- Name: g_object Type: TABLE DATA Owner: subho
--


INSERT INTO g_object VALUES ('Object','False',NULL,NULL,'Object','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('Attribute','False',NULL,NULL,'Attribute','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('Action','False',NULL,NULL,'Action','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('Transaction','False',NULL,NULL,'Transaction Log','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('System','False',NULL,NULL,'System','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('User','False',NULL,NULL,'User','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('Role','False',NULL,NULL,'Role','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('RoleAction','True','Role','Action','Role Action Association','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('RoleUser','True','Role','User','Role User Association','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('Password','False',NULL,NULL,'Password','False','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('ANY','False',NULL,NULL,'Any Object','True','False',1187817358,1187817358,0,0);
INSERT INTO g_object VALUES ('NONE','False',NULL,NULL,'No Object','True','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 25 (OID 2912737)
--
-- Name: g_object_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 26 (OID 2912777)
--
-- Name: g_attribute Type: TABLE DATA Owner: subho
--


INSERT INTO g_attribute VALUES ('Object','Name','String','True','True','True',NULL,NULL,10,'False','Object Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','Association','Boolean','False','False','False',NULL,'False',20,'False','Is this an association?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','Parent','String','False','False','False','@Object',NULL,30,'False','Parent Association','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','Child','String','False','False','False','@Object',NULL,40,'False','Child Association','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','Special','Boolean','False','False','False',NULL,'False',910,'True','Is this a special object?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this object deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Object','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Object','String','True','True','True','@Object',NULL,10,'False','Object name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Name','String','True','True','True',NULL,NULL,20,'False','Attribute Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','DataType','String','False','False','True','(AutoGen,TimeStamp,Boolean,Float,Integer,Currency,String)','String',30,'False','Data Type','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','PrimaryKey','Boolean','False','False','False',NULL,'False',50,'False','Is this a primary key?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Required','Boolean','False','False','False',NULL,'False',60,'False','Must this be non-null?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Fixed','Boolean','False','False','False',NULL,'False',70,'False','Is the Attribute Fixed?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Values','String','False','False','False',NULL,NULL,80,'False','List of allowed values','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','DefaultValue','String','False','False','False',NULL,NULL,90,'False','Default value','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Sequence','Integer','False','False','False',NULL,NULL,100,'False','Specifies ordering of attributes','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Hidden','Boolean','False','False','False',NULL,'False',110,'False','Is the Attribute Hidden by default?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Special','Boolean','False','False','False',NULL,'False',910,'True','Is this a special action?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this object deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Attribute','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','Object','String','True','True','True','@Object',NULL,10,'False','Object Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','Name','String','True','True','True',NULL,NULL,20,'False','Action Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','Display','Boolean','False','False','False',NULL,'False',30,'False','Should this action be displayed?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','Special','Boolean','False','False','False',NULL,'False',910,'True','Is this a special action?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this object deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Action','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Id','AutoGen','True','True','True',NULL,NULL,10,'False','Transaction Record Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Object','String','False','True','False',NULL,NULL,20,'False','Object','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Action','String','False','True','False',NULL,NULL,30,'False','Action','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Actor','String','False','True','False',NULL,NULL,40,'False','Authenticated User making the request','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Name','String','False','False','False',NULL,NULL,50,'False','Object Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Child','String','False','False','False',NULL,NULL,60,'False','Child Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Count','Integer','False','False','False',NULL,NULL,130,'False','Object Count','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Details','String','False','False','False',NULL,NULL,170,'True','Transaction Details','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this object deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Transaction','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','Name','String','True','True','True',NULL,NULL,10,'False','System Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','Version','String','False','True','False',NULL,NULL,20,'False','System Version','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this Object Deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('System','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','Name','String','True','True','True',NULL,NULL,10,'False','User Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','Special','Boolean','False','False','False',NULL,'False',910,'True','Is this a special user?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this Object Deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('User','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','Name','String','True','True','True',NULL,NULL,10,'False','Role Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','Description','String','False','False','False',NULL,NULL,900,'False','Description','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this Object Deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Role','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','Role','String','True','True','True','@Role',NULL,10,'False','Parent Role Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','Object','String','True','True','True','@Object',NULL,20,'False','Child Object Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','Name','String','True','True','True',NULL,NULL,30,'False','Child Action Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','Instance','String','False','False','False',NULL,'ANY',40,'False','Object Instance Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this Object Deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleAction','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','Role','String','True','True','True','@Role',NULL,10,'False','Parent Role Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','Name','String','True','True','True','@User',NULL,20,'False','Child User Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this Object Deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('RoleUser','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','User','String','True','True','True','@User',NULL,10,'False','User Name','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','Password','String','False','False','False',NULL,NULL,20,'False','Password','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','CreationTime','TimeStamp','False','False','False',NULL,NULL,950,'True','Time Created','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','ModificationTime','TimeStamp','False','False','False',NULL,NULL,960,'True','Last Updated','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','Deleted','Boolean','False','False','False',NULL,'False',970,'True','Is this Object Deleted?','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','RequestId','Integer','False','False','False',NULL,NULL,980,'True','Last Modifying Request Id','False','False',1187817358,1187817358,0,0);
INSERT INTO g_attribute VALUES ('Password','TransactionId','Integer','False','False','False',NULL,NULL,990,'True','Last Modifying Transaction Id','False','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 27 (OID 2912824)
--
-- Name: g_attribute_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 28 (OID 2912871)
--
-- Name: g_action Type: TABLE DATA Owner: subho
--


INSERT INTO g_action VALUES ('Object','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Object','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Object','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Object','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Object','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Attribute','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Attribute','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Attribute','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Attribute','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Attribute','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Action','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Action','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Action','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Action','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Action','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Transaction','Query','True',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Transaction','Undo','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Transaction','Redo','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('System','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('System','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('System','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('System','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('System','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('System','Refresh','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('User','Create','True',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('User','Query','True',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('User','Modify','True',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('User','Delete','True',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('User','Undelete','True',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Role','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Role','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Role','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Role','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Role','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleAction','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleAction','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleAction','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleAction','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleAction','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleUser','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleUser','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleUser','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleUser','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('RoleUser','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Password','Create','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Password','Query','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Password','Modify','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Password','Delete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('Password','Undelete','False',NULL,'False','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('ANY','ANY','False','Any Action','True','False',1187817358,1187817358,0,0);
INSERT INTO g_action VALUES ('NONE','NONE','False','No Action','True','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 29 (OID 2912906)
--
-- Name: g_action_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 30 (OID 2912941)
--
-- Name: g_transaction Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 31 (OID 2912980)
--
-- Name: g_transaction_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 32 (OID 2913019)
--
-- Name: g_system Type: TABLE DATA Owner: subho
--


INSERT INTO g_system VALUES ('Gold','2.1.4.2','Beta Release','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 33 (OID 2913052)
--
-- Name: g_system_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 34 (OID 2913085)
--
-- Name: g_user Type: TABLE DATA Owner: subho
--


INSERT INTO g_user VALUES ('ANY','Any User','True','False',1187817358,1187817358,0,0);
INSERT INTO g_user VALUES ('NONE','No User','True','False',1187817358,1187817358,0,0);
INSERT INTO g_user VALUES ('SELF','Authenticated User','True','False',1187817358,1187817358,0,0);
INSERT INTO g_user VALUES ('subho','Gold Admin','False','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 35 (OID 2913119)
--
-- Name: g_user_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 36 (OID 2913153)
--
-- Name: g_role Type: TABLE DATA Owner: subho
--


INSERT INTO g_role VALUES ('SystemAdmin','Can update or view any object','False',1187817358,1187817358,0,0);
INSERT INTO g_role VALUES ('Anonymous','Things that can be done by anybody','False',1187817358,1187817358,0,0);
INSERT INTO g_role VALUES ('OVERRIDE','A custom authorization method will be invoked','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 37 (OID 2913185)
--
-- Name: g_role_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 38 (OID 2913217)
--
-- Name: g_role_action Type: TABLE DATA Owner: subho
--


INSERT INTO g_role_action VALUES ('SystemAdmin','ANY','ANY','ANY','False',1187817358,1187817358,0,0);
INSERT INTO g_role_action VALUES ('Anonymous','ANY','Query','ANY','False',1187817358,1187817358,0,0);
INSERT INTO g_role_action VALUES ('Anonymous','Password','ANY','SELF','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 39 (OID 2913251)
--
-- Name: g_role_action_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 40 (OID 2913285)
--
-- Name: g_role_user Type: TABLE DATA Owner: subho
--


INSERT INTO g_role_user VALUES ('SystemAdmin','subho','False',1187817358,1187817358,0,0);
INSERT INTO g_role_user VALUES ('Anonymous','ANY','False',1187817358,1187817358,0,0);
--
-- Data for TOC Entry ID 41 (OID 2913317)
--
-- Name: g_role_user_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 42 (OID 2913349)
--
-- Name: g_password Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 43 (OID 2913381)
--
-- Name: g_password_log Type: TABLE DATA Owner: subho
--


--
-- Data for TOC Entry ID 44 (OID 2913413)
--
-- Name: g_key_generator Type: TABLE DATA Owner: subho
--


INSERT INTO g_key_generator VALUES ('Request',1);
INSERT INTO g_key_generator VALUES ('Transaction',1);
--
-- Data for TOC Entry ID 45 (OID 2913424)
--
-- Name: g_undo Type: TABLE DATA Owner: subho
--


