USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME,
        @in_servLine VARCHAR(MAX),
        @in_deps VARCHAR(MAX),
	    @in_pods VARCHAR(MAX),
	    @in_hubs VARCHAR(MAX),
	    @in_somdeps VARCHAR(MAX),
	    @in_somdivs VARCHAR(MAX)

--SET @StartDate = NULL
--SET @EndDate = NULL
--SET @StartDate = '2/3/2019 00:00 AM'
--SET @EndDate = '2/9/2019 11:59 PM'
--SET @StartDate = '5/27/2019 00:00 AM'
--SET @EndDate = '6/3/2019 11:59 PM'
SET @StartDate = '7/1/2018 00:00 AM'
--SET @EndDate = '6/10/2019 11:59 PM'
SET @EndDate = '6/13/2019 00:00 AM'

DECLARE @ServiceLine TABLE (ServiceLineId SMALLINT)

INSERT INTO @ServiceLine
(
    ServiceLineId
)
VALUES
--(1),--Digestive Health
--(2),--Heart and Vascular
--(3),--Medical Subspecialties
--(4),--Musculoskeletal
--(5),--Neurosciences and Behavioral Health
--(6),--Oncology
--(7),--Ophthalmology
--(8),--Primary Care
--(9),--Surgical Subspecialties
--(10),--Transplant
--(11) --Womens and Childrens
--(0)  --(All)
(1) --Digestive Health
--(1),--Digestive Health
--(2) --Heart and Vascular

SELECT @in_servLine = COALESCE(@in_servLine+',' ,'') + CAST(ServiceLineId AS VARCHAR(MAX))
FROM @ServiceLine

--SELECT @in_servLine

DECLARE @Department TABLE (DepartmentId NUMERIC(18,0))

INSERT INTO @Department
(
    DepartmentId
)
VALUES
-- (10210006)
--,(10210040)
--,(10210041)
--,(10211006)
--,(10214011)
--,(10214014)
--,(10217003)
--,(10239017)
--,(10239018)
--,(10239019)
--,(10239020)
--,(10241001)
--,(10242007)
--,(10242049)
--,(10243003)
--,(10244004)
--,(10348014)
--,(10354006)
--,(10354013)
--,(10354014)
--,(10354015)
--,(10354016)
--,(10354017)
--,(10354024)
--,(10354034)
--,(10354042)
--,(10354044)
--,(10354052)
--,(10354055)
 --(10214011)
 --(10210006)
 --(10280004) -- AUBL PEDIATRICS
 --(10341002) -- CVPE UVA RHEU INF PNTP
 --(10228008) -- NRDG MAMMOGRAPHY
 --(10381003) -- UVEC RAD CT
 --(10354032) -- UVBB PHYSICAL THER FL4
 --(10242018) -- UVPC PULMONARY
 --(10243003) -- UVHE DIGESTIVE HEALTH
 --(10239003) -- UVMS NEPHROLOGY
 --(10354015) -- UVBB PEDS ONCOLOGY CL
('0')  -- (All)
--(10210001) -- ECCC HEM ONC EAST
--,(10210002) -- ECCC HEM ONC WEST
--,(10210004) -- ECCC INFUSION CENTER
--,(10210013) -- ECCC RAD ONC CLINIC
--,(10210014) -- ECCC HEM ONC WOMENS
--,(10210028) -- ECCC MED DHC EAST CL
--,(10210035) -- ECCC GYN ONC WOMENS
--,(10210036) -- ECCC CARD DEVICE CL
--,(10210054) -- ECCC NEPHROLOGY WEST
--,(10211003) -- F415 RHEUMATOLOGY
--,(10211006) -- F415 ENDOCRINE
--,(10211012) -- F415 ALLERGY OTO
--,(10211013) -- F415 MED PITUITARY
--,(10211021) -- F415 NEUROSURGERY
--,(10212016) -- F500 CARDIOLOGY
--,(10212019) -- F500 ENDOCRINOLOGY
--,(10217002) -- JPA SLEEP LAB
--,(10217003) -- JPA UNIV MED ASSOCS
--,(10220001) -- LOID MEDICAL ASSOC
--,(10222003) -- LBSC KIDNEY TRANSPLANT
--,(10228003) -- NRDG MIDLIFE
--,(10228006) -- NRDG ECHO LAB
--,(10228007) -- NRDG ALLERGY
--,(10230004) -- ORUL UNIV PHYSICIANS
--,(10230005) -- ORUL MED NEPHROLOGY
--,(10230009) -- ORUL MED CARD
--,(10239003) -- UVMS NEPHROLOGY
--,(10239007) -- UVMS RAD MAMMOGRAPHY
--,(10239017) -- UVMS TRANSPLANT LIVER
--,(10239018) -- UVMS TRANSPLANT LUNG
--,(10239019) -- UVMS TRANSPLANT KIDNEY
--,(10239020) -- UVMS SURG TRANSPLANT
--,(10239023) -- UVMS MED GI CL NEPH
--,(10239024) -- UVMS ID TRANSPLANT
--,(10239026) -- UVMS ENDO TRANSPLANT
--,(10242005) -- UVPC DERMATOLOGY
--,(10242006) -- UVPC OB-GYN
--,(10242007) -- UVPC NEUROLOGY
--,(10242008) -- UVPC CARDIOLOGY
--,(10242018) -- UVPC PULMONARY
--,(10242019) -- UVPC PULM FUNC LAB
--,(10242047) -- ZZZZ SURG CARDTHORACIC
--,(10242050) -- UVPC ALLERGY PULM
--,(10243003) -- UVHE DIGESTIVE HEALTH
--,(10243009) -- UVHE ECG
--,(10243013) -- UVHE ELECTROPHYSIOLOGY
--,(10243020) -- UVHE GI MOTILITY
--,(10243097) -- UVHE MED CARD CL
--,(10243098) -- UVHE MED MONITORS
--,(10243114) -- UVHE ALLERGY DHC
--,(10244016) -- UVWC INFECTIOUS DIS
--,(10244023) -- UVWC MED GI CL
--,(10244024) -- UVWC ENDOCRN BARIATRIC
--,(10244028) -- UVWC GASTRO ID
--,(10244029) -- UVWC OBGYN ID
--,(10246004) -- WALL MED DH FAM MED
--,(10246005) -- WALL ENDO MED FAMILY
--,(10250001) -- UVBR NEUROSURGERY
--,(10256002) -- WSRS INFECTIOUS DIS
--,(10257001) -- CPNM MADISON PRIMARY C
--,(10257004) -- CPNM ENDOCRINE
--,(10275003) -- CPBR ALLERGY
--,(10277003) -- LGGH KIDNEY TRANSPLANT
--,(10280001) -- AUBL UVA SPTY CARE
--,(10280008) -- AUBL UVA CANC INF AUG
--,(10295005) -- CPSE CH CANC CTR
--,(10299001) -- CVSR NEPHROLOGY
--,(10299002) -- CVSR ENDOCRINE
--,(10306001) -- CVPJ UVA CANC CTR PTP
--,(10306003) -- CVPJ UVA CANC INF PTP
--,(10341001) -- CVPE UVA RHEU PANTOPS
--,(10341009) -- CVPE MED NEPHROLOGY
--,(10348003) -- ZCSC CARDIOLOGY
--,(10348005) -- ZCSC ENDOCRINE
--,(10348007) -- ZCSC NEPHROLOGY
--,(10348011) -- ZCSC PULMONARY
--,(10348039) -- ZCSC ALLERGY
--,(10353001) -- AUPN SP CARE NEPH
--,(10353003) -- AUPN SP CARE ENDO
--,(10353004) -- AUPN SP CARE PULM
--,(10353005) -- AUPN SP CARE RHEU
--,(10354007) -- UVBB PULM FUNC TESTING
--,(10354014) -- UVBB PEDS GASTRO CL
--,(10354018) -- UVBB PEDS CARD CL FL 6
--,(10354025) -- UVBB PEDS RESP MED FL6
--,(10354035) -- UVBB MATERNAL FETAL CL
--,(10354047) -- UVBB MED CARD CONGEN
--,(10354052) -- UVBB PEDS GI MOT
--,(10354074) -- UVBB PEDS RESP MED FL5
--,(10369001) -- CPSA CARDIOLOGY
--,(10374001) -- ROFR KIDNEY TRANSPLANT
--,(10377001) -- PAMS PRIMARY CARE
--,(10378001) -- WCVD KIDNEY TRANSPLANT
--,(10379900) -- UVML ENDOBRONC PROC
--,(10387001) -- CVSM PRIMARY CARE
--,(10390005) -- CPBE ENDOCRINE
--,(10399001) -- CVSN ENDOCRINE
-- (10210042) -- ECCC DERMATOLOGY WEST
--,(10210035) -- ECCC GYN ONC WOMENS
--,(10210001) -- ECCC HEM ONC EAST
--,(10210002) -- ECCC HEM ONC WEST
--,(10210014) -- ECCC HEM ONC WOMENS
--,(10210004) -- ECCC INFUSION CENTER
--,(10210028) -- ECCC MED DHC EAST CL
--,(10210030) -- ECCC NEURO WEST
--,(10210032) -- ECCC NSURG WEST
--,(10210026) -- ECCC OTO CL EAST
--,(10210027) -- ECCC OTO SPEECH EAST
--,(10210006) -- ECCC PALLIATIVE CLINIC
--,(10210057) -- ECCC PHYSMED&REHB WMS
--,(10210010) -- ECCC POSITIVE IMAGE
--,(10210017) -- ECCC PSY CL 1FL
--,(10210007) -- ECCC RAD CT
--,(10210045) -- ECCC RAD E&M CL WEST
--,(10210060) -- ECCC RAD E&M CL WMS
--,(10210018) -- ECCC RAD NUC MED CL
--,(10210008) -- ECCC RAD NUCLEAR
--,(10210013) -- ECCC RAD ONC CLINIC
--,(10210005) -- ECCC RAD ONC THERAPY
--,(10210009) -- ECCC RAD PET
--,(10210015) -- ECCC RONC PROCEDURE RM
--,(10210061) -- ECCC STRESS LAB
--,(10210016) -- ECCC SUPPORT SVCS 1FL
--,(10210022) -- ECCC SURG ONC EAST
--,(10210029) -- ECCC SURG ONC WEST
--,(10210052) -- ECCC UROLOGY WEST
;

SELECT @in_deps = COALESCE(@in_deps+',' ,'') + CAST(DepartmentId AS VARCHAR(MAX))
FROM @Department

--SELECT @in_deps

DECLARE @Pod TABLE (PodId VARCHAR(100))

INSERT INTO @Pod
(
    PodId
)
VALUES
--('1'),--Cancer
--('2'),--Musculoskeletal
--('3'),--Primary Care
--('4'),--Surgical Procedural Specialties
--('5'),--Transplant
--('6'),--Medical Specialties
--('7'),--Radiology
--('8'),--Heart and Vascular Center
--('9'),--Neurosciences and Psychiatry
--('10'),--Women's and Children's
--('12'),--CPG
--('13'),--UVA Community Cancer POD
--('14'),--Digestive Health
--('15'),--Ophthalmology
--('16') --Community Medicine
('0') --(All)
--('1') --Cancer
--('1'), --Cancer
--('10') --Women's and Children's
--('14')--,--Digestive Health
;

SELECT @in_pods = COALESCE(@in_pods+',' ,'') + CAST(PodId AS VARCHAR(MAX))
FROM @Pod

--SELECT @in_pods

DECLARE @Hub TABLE (HubId VARCHAR(66))

INSERT INTO @Hub
(
    HubId
)
VALUES
--('2'),--Non-Hub registration
--('3'),--ECCC
--('4'),--415 Fontaine
--('5'),--Battle
--('6'),--JPA
--('7'),--Northridge
--('8'),--PCC
--('9'),--West Complex
--('10') --500 Fontaine
('0') --(All)
--('3'),--ECCC
--('10') --500 Fontaine
--('6') --JPA
;

SELECT @in_hubs = COALESCE(@in_hubs+',' ,'') + CAST(HubId AS VARCHAR(MAX))
FROM @Hub

--SELECT @in_hubs

DECLARE @SOMDepartment TABLE (SOMDepartmentId VARCHAR(100))

INSERT INTO @SOMDepartment
(
    SOMDepartmentId
)
VALUES
--('0'),--(All)
--('57'),--MD-INMD Internal Medicine
--('98'),--MD-NERS Neurological Surgery
--('139'),--MD-OBGY Ob & Gyn
--('163'),--MD-ORTP Orthopaedic Surgery
--('194'),--MD-OTLY Otolaryngology
--('29'),--MD-PBHS Public Health Sciences
--('214'),--MD-PEDT Pediatrics
--('261'),--MD-PSCH Psychiatric Medicine
--('267'),--MD-RADL Radiology
--('292'),--MD-SURG Surgery
--('305'),--MD-UROL Urology
('0') --(All)
--('57')--,--MD-INMD Internal Medicine
--('292')--,--MD-SURG Surgery
--('47')--,--MD-ANES Anesthesiology
;

SELECT @in_somdeps = COALESCE(@in_somdeps+',' ,'') + CAST(SOMDepartmentId AS VARCHAR(MAX))
FROM @SOMDepartment

--SELECT @in_somdeps

DECLARE @SOMDivision TABLE (SOMDivisionId int)

INSERT INTO @SOMDivision
(
    SOMDivisionId
)
VALUES
(0)--,--(All)
--(14),--40445 MD-MICR Microbiology
--(22),--40450 MD-MPHY Mole Phys & Biophysics
--(30),--40415 MD-PBHS Public Health Sciences Admin
--(48),--40700 MD-ANES Anesthesiology
--(50),--40705 MD-DENT Dentistry
--(52),--40710 MD-DERM Dermatology
--(54),--40715 MD-EMED Emergency Medicine
--(56),--40720 MD-FMED Family Medicine
--(58),--40725 MD-INMD Int Med, Admin
--(60),--40730 MD-INMD Allergy
--(66),--40735 MD-INMD CV Medicine
--(68),--40745 MD-INMD Endocrinology
--(72),--40755 MD-INMD Gastroenterology
--(74),--40760 MD-INMD Gen, Geri, Pall, Hosp
--(76),--40761 MD-INMD Hospital Medicine
--(80),--40770 MD-INMD Hem/Onc
--(82),--40771 MD-INMD Community Oncology
--(84),--40775 MD-INMD Infectious Dis
--(86),--40780 MD-INMD Nephrology
--(88),--40785 MD-INMD Pulmonary
--(90),--40790 MD-INMD Rheumatology
--(98),--40746 MD-INMD Advanced Diabetes Mgt
--(101),--40800 MD-NERS Admin
--(111),--40820 MD-NERS CV Disease
--(113),--40830 MD-NERS Deg Spinal Dis
--(115),--40835 MD-NERS Gamma Knife
--(119),--40816 MD-NERS Minimally Invasive Spine
--(121),--40840 MD-NERS Multiple Neuralgia
--(123),--40825 MD-NERS Neuro-Onc
--(127),--40810 MD-NERS Pediatric
--(129),--40849 MD-NERS Pediatric Pituitary
--(131),--40806 MD-NERS Radiosurgery
--(138),--40850 MD-NEUR Neurology
--(142),--40860 MD-OBGY Ob & Gyn, Admin
--(144),--40865 MD-OBGY Gyn Oncology
--(146),--40870 MD-OBGY Maternal Fetal Med
--(148),--40875 MD-OBGY Reprod Endo/Infertility
--(150),--40880 MD-OBGY Midlife Health
--(152),--40885 MD-OBGY Northridge
--(154),--40890 MD-OBGY Primary Care Center
--(156),--40895 MD-OBGY Gyn Specialties
--(158),--40897 MD-OBGY Midwifery
--(163),--40900 MD-OPHT Ophthalmology
--(166),--40910 MD-ORTP Ortho Surg, Admin
--(168),--40915 MD-ORTP Adult Reconst
--(178),--40930 MD-ORTP Foot/Ankle
--(184),--40940 MD-ORTP Pediatric Ortho
--(188),--40950 MD-ORTP Spine
--(190),--40955 MD-ORTP Sports Med
--(192),--40960 MD-ORTP Hand Surgery
--(194),--40961 MD-ORTP Trauma
--(197),--40970 MD-OTLY Oto, Admin
--(201),--40980 MD-OTLY Audiology
--(208),--41005 MD-PATH Surgical Path
--(210),--41010 MD-PATH Clinical Pathology
--(212),--41015 MD-PATH Neuropathology
--(214),--41017 MD-PATH Research
--(219),--41025 MD-PEDT Pediatrics, Admin
--(223),--41035 MD-PEDT Cardiology
--(225),--41040 MD-PEDT Critical Care
--(227),--41045 MD-PEDT Developmental
--(229),--41050 MD-PEDT Endocrinology
--(233),--41056 MD-PEDT Bariatrics
--(237),--41058 MD-PEDT Adolescent Medicine
--(239),--41060 MD-PEDT Gastroenterology
--(241),--41065 MD-PEDT General Pediatrics
--(243),--41070 MD-PEDT Genetics
--(245),--41075 MD-PEDT Hematology
--(249),--41085 MD-PEDT Infectious Diseases
--(251),--41090 MD-PEDT Neonatology
--(253),--41095 MD-PEDT Nephrology
--(257),--41105 MD-PEDT Pulmonary
--(260),--41130 MD-PHMR Phys Med & Rehab
--(262),--41140 MD-PLSR Plastic Surgery
--(264),--41120 MD-PSCH Psychiatry and NB Sciences
--(270),--41160 MD-RADL Radiology, Admin
--(272),--41161 MD-RADL Community Division
--(274),--41165 MD-RADL Angio/Interv
--(276),--41166 MD-RADL Non-Invasive Cardio
--(278),--41170 MD-RADL Breast Imaging
--(280),--41175 MD-RADL Thoracoabdominal
--(282),--41180 MD-RADL Musculoskeletal
--(284),--41185 MD-RADL Neuroradiology
--(286),--41186 MD-RADL Interventional Neuroradiology (INR)
--(288),--41190 MD-RADL Nuclear Medicine
--(290),--41195 MD-RADL Pediatric Rad
--(295),--41150 MD-RONC Radiation Oncology
--(297),--41210 MD-SURG Surgery, Admin
--(310),--41250 MD-UROL Urology, Admin
--(314),--41255 MD-UROL Urology, General
--(327),--40480 MD-CDBT Ctr for Diabetes Tech
--(331),--40530 MD-CPHG Ctr for Public Health Genomics
--(373),--40204 MD-DMED School of Medicine Adm
--(435),--40230 MD-DMED Curriculum
--(435),--40250 MD-DMED Clin Performance Dev
--(435),--40265 MD-DMED Med Ed Chief of Staff
;

/*
DECLARE @SOMDivisionName TABLE (SOMDivisionNameId VARCHAR(150))

INSERT INTO @SOMDivisionName
(
    SOMDivisionNameId
)
VALUES
('0')--,--(All)
--('40204 MD-DMED School of Medicine Adm'),--Administration
--('40230 MD-DMED Curriculum'),--Undergraduate Medical Education
--('40250 MD-DMED Clin Performance Dev'),--Undergraduate Medical Education
--('40265 MD-DMED Med Ed Chief of Staff'),--Undergraduate Medical Education
--('40415 MD-PBHS Public Health Sciences Admin'),--MD-PBHS Public Health Sciences Admin
--('40445 MD-MICR Microbiology'),--40445 MD-MICR Microbiology
--('40450 MD-MPHY Mole Phys & Biophysics'),--40450 MD-MPHY Mole Phys & Biophysics
--('40480 MD-CDBT Ctr for Diabetes Tech'),--40480 MD-CDBT Ctr for Diabetes Tech
--('40530 MD-CPHG Ctr for Public Health Genomics'),--40530 MD-CPHG Ctr for Public Health Genomics
--('40700 MD-ANES Anesthesiology'),--40700 MD-ANES Anesthesiology
--('40705 MD-DENT Dentistry'),--40705 MD-DENT Dentistry
--('40710 MD-DERM Dermatology'),--40710 MD-DERM Dermatology
--('40715 MD-EMED Emergency Medicine'),--40715 MD-EMED Emergency Medicine
--('40720 MD-FMED Family Medicine'),--40720 MD-FMED Family Medicine
--('40725 MD-INMD Int Med, Admin'),--MD-INMD Int Med, Admin
--('40730 MD-INMD Allergy'),--MD-INMD Allergy
--('40735 MD-INMD CV Medicine'),--MD-INMD CV Medicine
--('40745 MD-INMD Endocrinology'),--MD-INMD Endocrinology
--('40746 MD-INMD Advanced Diabetes Mgt'),--MD-INMD Advanced Diabetes Mgt
--('40755 MD-INMD Gastroenterology'),--MD-INMD Gastroenterology
--('40760 MD-INMD Gen, Geri, Pall, Hosp'),--MD-INMD General Med
--('40761 MD-INMD Hospital Medicine'),--MD-INMD Hospital Medicine
--('40770 MD-INMD Hem/Onc'),--MD-INMD Hem/Onc
--('40771 MD-INMD Community Oncology'),--MD-INMD Community Oncology
--('40775 MD-INMD Infectious Dis'),--MD-INMD Infectious Dis
--('40780 MD-INMD Nephrology'),--MD-INMD Nephrology
--('40785 MD-INMD Pulmonary'),--MD-INMD Pulmonary
--('40790 MD-INMD Rheumatology'),--MD-INMD Rheumatology
--('40800 MD-NERS Admin'),--MD-NERS Admin
--('40806 MD-NERS Radiosurgery'),--MD-NERS Radiosurgery
--('40810 MD-NERS Pediatric'),--MD-NERS Pediatric
--('40816 MD-NERS Minimally Invasive Spine'),--MD-NERS Minimally Invasive Spine
--('40820 MD-NERS CV Disease'),--MD-NERS CV Disease
--('40825 MD-NERS Neuro-Onc'),--MD-NERS Neuro-Onc
--('40830 MD-NERS Deg Spinal Dis'),--MD-NERS Deg Spinal Dis
--('40835 MD-NERS Gamma Knife'),--MD-NERS Gamma Knife
--('40840 MD-NERS Multiple Neuralgia'),--MD-NERS Multiple Neuralgia
--('40849 MD-NERS Pediatric Pituitary'),--MD-NERS Pediatric Pituitary
--('40850 MD-NEUR Neurology'),--40850 MD-NEUR Neurology
--('40860 MD-OBGY Ob & Gyn, Admin'),--MD-OBGY Ob & Gyn, Admin
--('40865 MD-OBGY Gyn Oncology'),--MD-OBGY Gyn Oncology
--('40870 MD-OBGY Maternal Fetal Med'),--MD-OBGY Maternal Fetal Med
--('40875 MD-OBGY Reprod Endo/Infertility'),--MD-OBGY Reprod Endo/Infertility
--('40880 MD-OBGY Midlife Health'),--MD-OBGY Midlife Health
--('40885 MD-OBGY Northridge'),--MD-OBGY Northridge
--('40890 MD-OBGY Primary Care Center'),--MD-OBGY Primary Care Center
--('40895 MD-OBGY Gyn Specialties'),--MD-OBGY Gyn Specialties
--('40897 MD-OBGY Midwifery'),--MD-OBGY Midwifery
--('40900 MD-OPHT Ophthalmology'),--40900 MD-OPHT Ophthalmology
--('40910 MD-ORTP Ortho Surg, Admin'),--MD-ORTP Ortho Surg, Admin
--('40915 MD-ORTP Adult Reconst'),--MD-ORTP Adult Reconst
--('40930 MD-ORTP Foot/Ankle'),--MD-ORTP Foot/Ankle
--('40940 MD-ORTP Pediatric Ortho'),--MD-ORTP Pediatric Ortho
--('40950 MD-ORTP Spine'),--MD-ORTP Spine
--('40955 MD-ORTP Sports Med'),--MD-ORTP Sports Med
--('40960 MD-ORTP Hand Surgery'),--MD-ORTP Hand Surgery
--('40961 MD-ORTP Trauma'),--MD-ORTP Trauma
--('40970 MD-OTLY Oto, Admin'),--MD-OTLY Oto, Admin
--('40980 MD-OTLY Audiology'),--MD-OTLY Audiology
--('41005 MD-PATH Surgical Path'),--MD-PATH Surgical Path
--('41010 MD-PATH Clinical Pathology'),--MD-PATH Clinical Pathology
--('41015 MD-PATH Neuropathology'),--MD-PATH Neuropathology
--('41017 MD-PATH Research'),--MD-PATH Research
--('41025 MD-PEDT Pediatrics, Admin'),--MD-PEDT Pediatrics, Admin
--('41035 MD-PEDT Cardiology'),--MD-PEDT Cardiology
--('41040 MD-PEDT Critical Care'),--MD-PEDT Critical Care
--('41045 MD-PEDT Developmental'),--MD-PEDT Developmental
--('41050 MD-PEDT Endocrinology'),--MD-PEDT Endocrinology
--('41056 MD-PEDT Bariatrics'),--MD-PEDT Bariatrics
--('41058 MD-PEDT Adolescent Medicine'),--MD-PEDT Adolescent Medicine
--('41060 MD-PEDT Gastroenterology'),--MD-PEDT Gastroenterology
--('41065 MD-PEDT General Pediatrics'),--MD-PEDT General Pediatrics
--('41070 MD-PEDT Genetics'),--MD-PEDT Genetics
--('41075 MD-PEDT Hematology'),--MD-PEDT Hematology
--('41085 MD-PEDT Infectious Diseases'),--MD-PEDT Infectious Diseases
--('41090 MD-PEDT Neonatology'),--MD-PEDT Neonatology
--('41095 MD-PEDT Nephrology'),--MD-PEDT Nephrology
--('41105 MD-PEDT Pulmonary'),--MD-PEDT Pulmonary
--('41120 MD-PSCH Psychiatry and NB Sciences'),--MD-PSCH Psychiatry and NB Sciences
--('41130 MD-PHMR Phys Med & Rehab'),--41130 MD-PHMR Phys Med & Rehab
--('41140 MD-PLSR Plastic Surgery'),--41140 MD-PLSR Plastic Surgery
--('41150 MD-RONC Radiation Oncology'),--41150 MD-RONC Radiation Oncology
--('41160 MD-RADL Radiology, Admin'),--MD-RADL Radiology, Admin
--('41161 MD-RADL Community Division'),--MD-RADL Community Division
--('41165 MD-RADL Angio/Interv'),--MD-RADL Angio/Interv
--('41166 MD-RADL Non-Invasive Cardio'),--MD-RADL Non-Invasive Cardio
--('41170 MD-RADL Breast Imaging'),--MD-RADL Breast Imaging
--('41175 MD-RADL Thoracoabdominal'),--MD-RADL Thoracoabdominal
--('41180 MD-RADL Musculoskeletal'),--MD-RADL Musculoskeletal
--('41185 MD-RADL Neuroradiology'),--MD-RADL Neuroradiology
--('41186 MD-RADL Interventional Neuroradiology (INR)'),--MD-RADL Interventional Neuroradiology (INR)
--('41190 MD-RADL Nuclear Medicine'),--MD-RADL Nuclear Medicine
--('41195 MD-RADL Pediatric Rad'),--MD-RADL Pediatric Rad
--('41210 MD-SURG Surgery, Admin'),--MD-SURG Surgery, Admin
--('41250 MD-UROL Urology, Admin'),--MD-UROL Urology, Admin
--('41255 MD-UROL Urology, General'),--MD-UROL Urology, General
;
*/

SELECT @in_somdivs = COALESCE(@in_somdivs+',' ,'') + CAST(SOMDivisionId AS VARCHAR(MAX))
FROM @SOMDivision

/*
SELECT @in_somdivs = COALESCE(@in_somdivs+',' ,'') + CAST(SOMDivisionNameId AS VARCHAR(MAX))
FROM @SOMDivisionName
*/

--SELECT @in_somdivs

--ALTER PROCEDURE [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily_Summary]
--    (
--     @StartDate SMALLDATETIME = NULL,
--     @EndDate SMALLDATETIME = NULL,
--     @in_servLine VARCHAR(MAX),
--     @in_deps VARCHAR(MAX),
--	 @in_pods VARCHAR(MAX),
--	 @in_hubs VARCHAR(MAX),
--     @in_somdeps VARCHAR(MAX),
--	 @in_somdivs VARCHAR(MAX)
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure Rptg.uspSrc_AmbOpt_NoShowRate_SSRS_Daily_Summary
--WHO : Tom Burgan
--WHEN: 6/7/19
--WHY : Report scheduled appointment No Show Rate from Cadence.
-- 
--	Metric Calculations
--
--		Note: "SUM" can be interpreted as "SUM(event_count) WHERE ...."
--
-- No Show Rate
--				(SUM(appt_event_No_Show = 1) + SUM(appt_event_Canceled_Late = 1))
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDM_App.Stage.Scheduled_Appointment
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--				DS_HSDW_Prod.Rptg.vwDim_Patient
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--				DS_HSDW_Prod.Rptg.vwDim_Physcn
--				DS_HSDW_Prod.Rptg.vwRef_Service_Line
--				DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye
--				DS_HSDM_App.Stage.AmbOpt_Excluded_Department
--				DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined
--				DS_HSDW_Prod.Rptg.vwDim_Date
--                
--      OUTPUTS:  [Rptg].[uspSrc_AmbOpt_NoShowRate_SSRS_Daily_Summary]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         06/07/2019 - TMB - create stored procedure
--         06/12/2019 - TMB - edit logic: StartDate and EndDate arguments may not have time values
--		   07/01/2019 - TMB - change logic for setting SOM hierarchy values
--************************************************************************************************************************

    SET NOCOUNT ON;

-------------------------------------------------------------------------------
DECLARE @locStartDate SMALLDATETIME,
        @locEndDate SMALLDATETIME

SET @locStartDate = CAST(CAST(@StartDate AS DATE) AS SMALLDATETIME) + CAST(CAST('00:00:00' AS TIME) AS SMALLDATETIME)
SET @locEndDate   = CAST(DATEADD(MINUTE,-1,CAST((DATEADD(DAY,1,CAST(@EndDate AS DATE))) AS SMALLDATETIME)) AS SMALLDATETIME)

DECLARE @tab_servLine TABLE
(
    Service_Line_Id int
);
INSERT INTO @tab_servLine
SELECT Param
FROM ETL.fn_ParmParse(@in_servLine, ',');
DECLARE @tab_pods TABLE
(
    pod_id VARCHAR(66)
);
INSERT INTO @tab_pods
SELECT Param
FROM ETL.fn_ParmParse(@in_pods, ',');
DECLARE @tab_hubs TABLE
(
    hub_id VARCHAR(66)
);
INSERT INTO @tab_hubs
SELECT Param
FROM ETL.fn_ParmParse(@in_hubs, ',');
DECLARE @tab_deps TABLE
(
    epic_department_id NUMERIC(18,0)
);
INSERT INTO @tab_deps
SELECT Param
FROM ETL.fn_ParmParse(@in_deps, ',');
DECLARE @tab_somdeps TABLE
(
    som_department_id int
);
INSERT INTO @tab_somdeps
(
    som_department_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdeps, ',');
--DECLARE @tab_somdivs TABLE
--(
--    som_division_name_id VARCHAR(150)
--);
--INSERT INTO @tab_somdivs
--(
--    som_division_name_id
--)
--SELECT Param
--FROM ETL.fn_ParmParse(@in_somdivs, ',');
DECLARE @tab_somdivs TABLE
(
    som_division_id int
);
INSERT INTO @tab_somdivs
(
    som_division_id
)
SELECT Param
FROM ETL.fn_ParmParse(@in_somdivs, ',');

SELECT SUM(CASE WHEN evnts.appt_event_Canceled = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45) THEN 1 ELSE 0 END) AS [Appointment],
	   SUM(CASE WHEN (evnts.appt_event_No_Show = 1 OR evnts.appt_event_Canceled_Late = 1) THEN 1 ELSE 0 END) AS [No Show]

FROM

    (
        SELECT DISTINCT
            main.epic_pod AS pod_id,
            main.epic_hub AS hub_id,
            main.epic_department_id,
            main.service_line_id,
            main.opnl_service_id,
            main.APPT_DT,
            main.PAT_ENC_CSN_ID,
                                                 -- Appt Status Flags
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'No Show' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_No_Show,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled Late' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Canceled_Late,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled' ))
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C = 3)
                    AND (main.CANCEL_INITIATOR = 'PROVIDER')
                ) THEN
                    1
                ELSE
                    0
            END AS appt_event_Provider_Canceled,
                                                 -- Calculated columns
-- Assumes that there is always a referral creation date (CHANGE_DATE) documented when a referral entry date (ENTRY_DATE) is documented
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled','Canceled Late' ))
                ) THEN
                    DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT)
                ELSE
                    CAST(NULL AS INT)
            END AS Cancel_Lead_Days,
			main.som_department_id,
			main.som_division_id,
	        main.som_division_name

        FROM
        ( --main
            SELECT
			       appts.RPT_GRP_SIX AS epic_pod,
				   appts.RPT_GRP_SEVEN AS epic_hub,
				   appts.DEPARTMENT_ID AS epic_department_id,
				   mdm.service_line_id,
                   mdm.opnl_service_id,
                   --Select
                   appts.APPT_STATUS_FLAG,
                   appts.APPT_STATUS_C,
				   appts.CANCEL_INITIATOR,
                   appts.APPT_DT,
                   appts.PAT_ENC_CSN_ID,
                   appts.APPT_CANC_DTTM,
				   -- SOM
				   physcn.SOM_department_id AS som_department_id,
				   physcn.SOM_division_id AS som_division_id,
				   physcn.SOM_division_name AS som_division_name

            FROM Stage.Scheduled_Appointment AS appts
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
                    ON ser.PROV_ID = appts.PROV_ID
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
                    ON pat.sk_Dim_Pt = appts.sk_Dim_Pt
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                    ON appts.DEPARTMENT_ID = mdm.epic_department_id
                LEFT OUTER JOIN
                (
                    SELECT DISTINCT
                        EPIC_DEPARTMENT_ID,
                        SERVICE_LINE,
                        PFA_POD,
                        HUB,
						BUSINESS_UNIT,
						LOC_ID,
						REV_LOC_NAME
                    FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                ) AS mdmloc
                    ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
                LEFT JOIN
                (
                    SELECT sk_Dim_Physcn,
                           UVaID,
                           Service_Line
                    FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
                    WHERE current_flag = 1
                ) AS doc
                    ON ser.sk_Dim_Physcn = doc.sk_Dim_Physcn
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
                    ON physsvc.Physician_Roster_Name = CASE
                                                           WHEN (ser.sk_Dim_Physcn > 0) THEN
                                                               doc.Service_Line
                                                           ELSE
                                                               'No Value Specified'
                                                       END
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp
				    ON entryemp.EMPlye_Usr_ID = appts.APPT_ENTRY_USER_ID
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye cancemp
				    ON cancemp.EMPlye_Usr_ID = appts.APPT_CANC_USER_ID

                -- -------------------------------------
                -- Excluded departments--
                -- -------------------------------------
                LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
				    ON excl.DEPARTMENT_ID = appts.DEPARTMENT_ID

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = doc.sk_Dim_Physcn

            WHERE (appts.APPT_DT >= @locStartDate
              AND appts.APPT_DT < @locEndDate)
			AND excl.DEPARTMENT_ID IS NULL

        ) AS main
    ) evnts
INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date date_dim
    ON (date_dim.day_date = CAST(evnts.APPT_DT AS SMALLDATETIME))

WHERE ((evnts.appt_event_Canceled = 0)
       OR ((evnts.appt_event_No_Show = 1)
	       OR (evnts.appt_event_Canceled_Late = 1)
		   OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 45)
		  )
	  )
	  AND date_dim.day_date >= @locStartDate
      AND date_dim.day_date < @locEndDate
      AND
      (
          @in_servLine = 0
          OR (COALESCE(evnts.service_line_id, evnts.opnl_service_id) IN (SELECT Service_Line_Id FROM @tab_servLine))
			 
      )
      AND
      (
          @in_pods = 0
          OR (evnts.pod_id IN (SELECT pod_id FROM @tab_pods))
      )
      AND
      (
          @in_hubs = 0
          OR (evnts.hub_id IN (SELECT hub_id FROM @tab_hubs))
      )
      AND
      (
          @in_deps = 0
          OR (evnts.epic_department_id IN (SELECT epic_department_id FROM @tab_deps))
      )
      AND
      (
          @in_somdeps = 0
          OR (evnts.som_department_id IN (SELECT som_department_id FROM @tab_somdeps))
      )
      AND
      (
          @in_somdivs = 0
          OR (evnts.som_division_id IN (SELECT som_division_id FROM @tab_somdivs))
      );

GO


