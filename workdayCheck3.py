# coding=utf-8
import pandas as pd
import numpy as np
import glob
import sys
import os
import datetime
import re
# from openpyxl import Workbook
# from openpyxl.styles import PatternFill

# birthRight->based not 1-1 so handled

dtLocalNow = datetime.datetime.now()
dateRunString = dtLocalNow.strftime("%Y%m%d")
dateFmt_String = dtLocalNow.strftime("%Y-%m-%d")
dateExcelString = dtLocalNow.strftime("%m%d")
printEmplyeeId = ["108213"]


def roleCheck(folderPath, prefixBaseName="role_result",
              checkColnHead=["Employee ID #", "Role Name"], appName="Workday"):
    # , skiprows=[0,1,2,3,4]
    ##############
    # define Path
    ##########
    IEEPathAry = glob.glob(folderPath+"/INT030_EE_Role*")
    if len(IEEPathAry) > 0:
        IEEPath = IEEPathAry[0]
    brPathAry = glob.glob(
        folderPath+"/BirthRight_Role_Entitlement_-_Weekly_Report*")
    if len(brPathAry) > 0:
        brPath = brPathAry[0]

    activeEmplyeeListPathAry = glob.glob(
        folderPath+"/MGM IT - Active Employee List*")
    if len(activeEmplyeeListPathAry) > 0:
        activeEmplyeeListPath = activeEmplyeeListPathAry[0]
    ticketPathAry = glob.glob(folderPath+"/sc_*.xlsx")
    if len(ticketPathAry) > 0:
        ticketPath = ticketPathAry[0]
    role1PathAry = glob.glob(folderPath+"/role_result*")
    print(role1PathAry)
    if len(role1PathAry) > 0:
        role1Path = role1PathAry[0]

    ####################################
    # get active Employee Data
    ###################################
    # activeDf=pd.read_csv(activeEmplyeeListPath,dtype=str)
    activeDf = pd.DataFrame(pd.read_excel(
        activeEmplyeeListPath, dtype=str, skiprows=[0, 1]))
    # Emp ID
    # Business Entity,Company Code
    activeDf.rename(columns={'Emp ID': 'Employee ID #',
                             "Company - ID": "Company Code", "Division ID": "DIVISION CODE", "Section ID": "SECTION CODE","Department ID":"DEPARTMENT CODE"}, inplace=True)

    activeDf["Preferred Name"] = activeDf["Preferred Name"] + \
        " "+activeDf["Last Name"]

    activeSubDf = activeDf[['Employee ID #', "Preferred Name", "Job Profile Code",
                            "Business Entity", "Company Code", "DIVISION CODE", "DEPARTMENT CODE", "SECTION CODE"]]

    # print("===activeDF===")
    # print(activeSubDf.head())
    ######################
    # company code =001----> (int)1----> (str)1
    #####################
    activeSubDf["Company Code"] = activeSubDf["Company Code"].astype(
        int).astype(str)
    birthRightDf = pd.read_csv(brPath, dtype=str)

    ####################################
    # get IEE role assignment data
    ###################################
    IEEDf = pd.DataFrame(pd.read_excel(IEEPath, dtype=str, skiprows=[0, 1, 2]))
    
    ####################################
    # exclude the required role name
    ###################################s
    exclusiveRoleNameList = ["Alternate Owner", "Dataset Editor", "Dataset Owner",
                             "Hierarchy Owner", "MGM Hiring Manager", "Owner", "Performance Manager",
                             "Pride Reviewer", "Primary Recruiter", "Superior"]
    IEESubDf = IEEDf[~IEEDf["Role Name"].isin(exclusiveRoleNameList)]
    ieeHeader = IEEDf.columns

    ####################################
    # get last  role checked assignment data
    ###################################
    role1Df = pd.DataFrame(pd.read_excel(role1Path, dtype=str))
    ####################################
    # clean up the last "new check" from Remark8
    ###################################
    lastCheckedRoleDf = role1Df.loc[role1Df["Remarks"].isin(["new check", "new check, org in desc", "new check, in description",
"new check, in description, but no org confirmed", "new check, Remote Access for Workday Desktop View"])]
    role1Df.loc[lastCheckedRoleDf.index, "Remarks"] = np.nan
    roleHeader = role1Df.columns
    role2Df = pd.DataFrame()
    # role1Df=pd.read_csv(folderPath+"/WD_role_assignment1.csv")
    # role2Df=pd.read_csv(folderPath+"/WD_role_assignment2.csv")
    # ticketDf=pd.read_csv(folderPath+"/ticket.csv")
    ####################################
    # get ticket data
    ###################################
    ticketDf = pd.DataFrame(pd.read_excel(ticketPath, dtype=str))
    # WD_role_assignment1
    # BirthRight_Role_Entitlement_-_Weekly_Report
    # INT030_EE_Role_Assignment

    ###################################################
    # begin to generate the new checked role assignment data
    ##################################################
    role2Df[roleHeader[1]] = IEESubDf[ieeHeader[0]].astype(
        str)  # Employee ID #
    # role2Df[roleHeader[2]] = IEESubDf[ieeHeader[1]].astype(
    #     str)  # Preferred Name in General Display Format
    # Role Name,Organization,Organization_Reference_ID
    role2Df[roleHeader[3]] = IEESubDf[ieeHeader[7]].astype(str)  # Role Name
    role2Df[roleHeader[4]] = IEESubDf[ieeHeader[8]].astype(str)  # rganization
    role2Df[roleHeader[5]] = IEESubDf[ieeHeader[9]].astype(
        str)  # organization_Reference_ID
    print("====role2====")
    print(role2Df.head())

    ###################################################
    # role3:
    # according employ ID, join acitve employee data and role2Data from IEE
    ##################################################
    role3Df = pd.merge(role2Df, activeSubDf, on='Employee ID #')
    role3Df[roleHeader[0]] = role3Df.apply(lambda row: concatIEERoleIdInfo(row['Employee ID #'], row['Role Name'], row['Organization_Reference_ID'],
    row['Job Profile Code'], row['Business Entity'],
    row['Company Code']), axis=1)

    ###################################################
    # role4Df:
    # according the totalIdInfo(defomed by function concatIEERoleIdInfo),
    #  join last checked role assignment data and role3Data from IEE
    ##################################################
    print("---role3Df-----" )
    print(role3Df.columns)
    role4Df = pd.merge(role3Df, role1Df[[
                       roleHeader[0], 'Comment', 'Remarks', 'Compliance Checking']], on=roleHeader[0], how="left")
    ##################################################
    # role4Df:
    # assign "*inacative" comment to inactive organization
    ##################################################
    print("===print inactive=====")
    role4SubDf = role4Df.loc[role4Df["Organization"].str.contains("inactive")]
    print(role4SubDf.index)
    role4Df.loc[role4SubDf.index, 'Comment'] = "*inactive organization"
    role4Df.loc[:,"Application"]=appName
    ##################################################
    # unfoundDf:
    # handleing the NaN comment
    ##################################################
    unfoundDf = role4Df.loc[role4Df["Comment"].isna()]
    # .isin(["","#N/A"])

    print("unfoundDF  %s" % unfoundDf.shape[0])
    for i in unfoundDf.index:
        # 'Employee ID #'
        # roleName=unfoundDf.loc[i,"Role Name"]
        empId = unfoundDf.loc[i, 'Employee ID #']
        roleName = unfoundDf.loc[i, "Role Name"]
        roleName = unfoundDf.loc[i, "Role Name"]
        organizationReferenceID = unfoundDf.loc[i, "Organization_Reference_ID"]
        roleName = roleName.replace("(", r"\(")
        roleName = roleName.replace(")", r"\)")
        roleNameList = roleName.split(" ")
        roleNameSearchRegex = r"[\s]{1,3}".join(roleNameList)
        roleNameSearchRegex = r"(?i)" + roleNameSearchRegex
        businessEntity = unfoundDf.loc[i, "Business Entity"]
        jobProfile = unfoundDf.loc[i, "Job Profile Code"]
        organization = unfoundDf.loc[i, "Organization"]
        organizationIdAry = organization.split(" ")

        if "services".upper() in businessEntity.upper():
            businessEntityRegx = businessEntity.replace(
                "Services", r"Servic[e]{0,1}[s]{0,1}")
        elif "service".upper() in businessEntity.upper():
            businessEntityRegx = businessEntity.replace(
                "Service", r"Servic[e]{0,1}[s]{0,1}")
        elif "servic".upper() in businessEntity.upper():
            businessEntityRegx = businessEntity.replace(
                "Servic", r"Servic[e]{0,1}[s]{0,1}")
        else:
            businessEntityRegx = businessEntity
        businessEntityRegx = r"(?i)((" + businessEntityRegx+r")|ALL)"
        jobProfile = unfoundDf.loc[i, "Job Profile Code"]
        # if jobProfile=="201105":
        #     businessEntityRegx=r"008 Corporate Servic[e]{0,1}"
        companyCode = unfoundDf.loc[i, "Company Code"]

        divisionCode = unfoundDf.loc[i, "DIVISION CODE"]
        departmentCode = unfoundDf.loc[i, "DEPARTMENT CODE"]
        sectionCode = unfoundDf.loc[i, "SECTION CODE"]
        if len(organizationIdAry) > 0:
            if organizationIdAry[0].isdigit():
                orgIdNum = organizationIdAry[0]
            else:
                orgIdNum = organization
        if len(jobProfile) < 6:
            jobProfile = "0"+jobProfile
        # organization = unfoundDf.loc[i, "Organization"]

        # foundBRDf = birthRightDf.loc[
        #     (birthRightDf["Entitlement"].str.contains(
        #         roleNameSearchRegex)) &
        #     (birthRightDf["Job Profile"].str.contains(jobProfile)) &
        #     (birthRightDf["Business Entity"].str
        #      .contains(r"(?i)" + businessEntity+r"|ALL"))
        # ]
        if "Cotai".upper() not in appName.upper():
            appNameRegex = r"(?i)(?!Cotai)[\s]{0,2}" + appName
        else:
            appNameRegex = r"(?i)"+appName
        foundBRDf = birthRightDf.loc[
            (birthRightDf["Entitlement"].str.contains(
                roleNameSearchRegex)) &
            (
                birthRightDf["Job Profile"].str.contains(jobProfile)
                | (birthRightDf["Job Profile"].str.contains(r"(?i)"+"DIVISION CODE:"+divisionCode)) |
                (birthRightDf["Job Profile"].str.contains(
                    r"(?i)"+"DEPARTMENT CODE:"+departmentCode))
                | (birthRightDf["Job Profile"].str.contains(r"(?i)"+"SECTION CODE:"+sectionCode))
            ) &
            (birthRightDf["Business Entity"].str
             .contains(businessEntityRegx))
            & (birthRightDf["Applicationname"].str
                .contains(appNameRegex))
            &
            (birthRightDf["Company Code"].str
             .contains(r"(?i)(" + companyCode + r"|ALL)"))
        ]

        foundBRDf_withOrg = foundBRDf.loc[
            (foundBRDf["Entitlement"].str.contains(organizationReferenceID)) |
            (foundBRDf["Entitlement"].str.contains(orgIdNum))
        ]

        if str(empId) in printEmplyeeId:
            print("\n\n=------ print br df--- ")
            print(foundBRDf)
            print(foundBRDf_withOrg)
        # foundBRDf = birthRightDf.loc[\
        #     (birthRightDf["Entitlement"]str.contains(businessEntity)) &
        #                                 (birthRightDf["Job Profile"].str.contains(jobProfile))]
        if foundBRDf_withOrg.shape[0] == 1:
            role4Df.loc[i, "Comment"] = "BR"
            role4Df.loc[i, "Compliance Checking"] = "BR confirmed"
            if pd.isna(role4Df.loc[i, "Remarks"]) or role4Df.loc[i, "Remarks"] in ["", "#N/A"]:
                role4Df.loc[i, "Remarks"] = "new check"
            else:
                role4Df.loc[i, "Remarks"] = "new check " + \
                    role4Df.loc[i, "Remarks"]
        elif foundBRDf_withOrg.shape[0] == 0:
            FoundFlag2 = None
            role4Df, FoundFlag = descSearch(
                role4Df, i, ticketDf, roleName, empId, organization, organizationReferenceID, businessEntity)

            if not FoundFlag:
                role4Df, FoundFlag2 = descLooseSearch(
                    role4Df, i, ticketDf, roleName, empId, organization, organizationReferenceID, businessEntity)
            if FoundFlag2 is not None and not FoundFlag2 and not FoundFlag and foundBRDf.shape[0] == 1:
                role4Df.loc[i, "Comment"] = "BR"
                role4Df.loc[i, "Compliance Checking"] = "BR confirmed"
                if pd.isna(role4Df.loc[i, "Remarks"]) or role4Df.loc[i, "Remarks"] in ["", "#N/A"]:
                    role4Df.loc[i,
                                "Remarks"] = "new check, but no org confirmed"
                else:
                    role4Df.loc[i, "Remarks"] = "new check, but no org confirmed " + \
                        role4Df.loc[i, "Remarks"]

        else:
            role4Df, FoundFlag = descSearch(
                role4Df, i, ticketDf, roleName, empId, organization, organizationReferenceID, businessEntity)
            FoundFlag2 = None
            if FoundFlag:

                role4Df.loc[i, "Comment"] = "BR>1, need to check "+role4Df.loc[i, "Comment"]

            elif not FoundFlag:
                role4Df, FoundFlag2 = descLooseSearch(
                    role4Df, i, ticketDf, roleName, empId, organization, organizationReferenceID, businessEntity)

                if FoundFlag2 is not None and FoundFlag2:

                    role4Df.loc[i, "Comment"] = "BR>1, need to check " + \
                        role4Df.loc[i, "Comment"]
                elif FoundFlag2 is not None and not FoundFlag2:
                    role4Df.loc[i, "Comment"] = \
                        "BR>1, need to check"
                    if pd.isna(role4Df.loc[i, "Remarks"]) or role4Df.loc[i, "Remarks"] in ["", "#N/A"]:
                        role4Df.loc[i, "Remarks"] = "new check"
                    else:
                        role4Df.loc[i, "Remarks"] = "new check " + \
                            role4Df.loc[i, "Remarks"]
                    role4Df.loc[i, "Compliance Checking"] = ""
    ##################################################
    # reorder the columns for output  #
    ##################################################
    role4Df = role4Df[
        [roleHeader[0], "Employee ID #",
         "Preferred Name",
         "Role Name", "Organization",	"Organization_Reference_ID",
            'Comment', 'Remarks', 'Compliance Checking',
         "Job Profile Code", "Business Entity", "Company Code","DIVISION CODE","DEPARTMENT CODE","SECTION CODE","Application"
         ]
    ]
    # print(role4Df.loc[role4Df["Employee ID #"]=="103025"])
    # role4Df.to_csv("111.csv",index=False)
    writer = pd.ExcelWriter(folderPath+"/role_result_%s.xlsx" %
                            dateExcelString, engine='xlsxwriter')
    role4Df.to_excel(writer, index=False, sheet_name='%s-EC' %
                     dateExcelString)

    workbook = writer.book
    worksheet = writer.sheets['%s-EC' %
                              dateExcelString]

    # adjust the column widths based on the content
    wrap_format = workbook.add_format({'text_wrap': True})
    widthList = [23.21, 11.86, 21.57, 16.64, 34.07,
                 11.79, 12.79, 8.5, 7.07, 8.5,  8.5, 8.5, 8.5,16.3, 8.5,8.5]

    for i, col in enumerate(role4Df.columns):
        # width = max(df[col].apply(lambda x: len(str(x))).max(), len(col))
        worksheet.set_column(i, i, widthList[i], wrap_format)

    my_formats = {
        'Employee ID #': "#C0E6F5",
        'Preferred Name': "#C0E6F5",
        'Role Name': "#C0E6F5",
        'Organization': "#C0E6F5",
        'Organization_Reference_ID': "#C0E6F5",
        'Job Profile Code': "#FBE2D5",
        'Business Entity': "#FBE2D5",
        'Company Code': "#FBE2D5",
        'Job Profile_Security Group': "#F1A983",
        'CC Checking(From BR)': "#94DCF8",
        'BE Checking(From BR)': '#94DCF8',
        "Comment": "#92D050",
        "Remarks": "#92D050",
        "Compliance Checking": "#92D050",
        "Application":"#C0E6F5",
    }

    for col_num, value in enumerate(role4Df.columns.values):
        if col_num in [0]:
            print(col_num, value)
            format1 = workbook.add_format({'bg_color': "#C0E6F5"})
            worksheet.write(0, col_num, dateFmt_String, format1)
        elif value in my_formats.keys():
            format1 = workbook.add_format({'bg_color': my_formats[value]})
            worksheet.write(0, col_num, value, format1)
        else:
            format1 = workbook.add_format({})
            worksheet.write(0, col_num, value, format1)
    (max_row, max_col) = role4Df.shape
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    # save the Excel file
    try:
        writer._save()
    except:
        writer.save()
    return folderPath+"/role_result_%s.xlsx" %dateExcelString
    # role4Df.to_excel("result.xlsx",index=False)


def descSearch(resultDf, i, ticketDf, roleName, empId, organization, organizationId, businessEntity):
    ticketBRDf = ticketDf.loc[ticketDf["Item"] == "Birthright Access"]
    ticketEptDf = ticketDf.loc[ticketDf["Item"] == "Exception Access"]
    # if "Selective IP Unrestricted".lower() in roleName.lower():
    #     roleName2 = roleName.lower().replace("Selective IP Unrestricted".lower(),"Remote Access".lower())
    # else:
    appName = resultDf.loc[resultDf.index[0], "Application"]
    # appName="Workday"

    if "Cotai".upper() not in appName.upper():
        appNameRegex = r"(?i)(Application Name|應用程式名稱)[^\n]*[:,]{1}[\s]{0,4}((?!Cotai) %s)[\s]{0,2}" % appName
    else:
        appNameRegex = r"(?i)(Application Name|應用程式名稱)[^\n]*[:,]{1}[\s]{0,4}"+appName
    roleName2 = roleName
    roleNameList = roleName2.split(" ")
    roleNameSearchRegex = r"[\s]{0,3}".join(roleNameList)
    roleNameSearchRegex = r"(?i)(權限|Role Permission)[:：\s]{1,4}[^\n]*" + roleNameSearchRegex +\
        r"[^\n]*"
    if organization != "":
        organization = organization.replace(",", r"[,]{0,2}")
        if "limted" in organization.lower():
            organization = organization.replace(
                "Limted", r"(Limited|Limted|Ltd)[.]{0,1}")
        elif "limited" in organization.lower():
            organization = organization.replace(
                "Limited", r"(Limited|Limted|Ltd)[.]{0,1}")
        elif "ltd" in organization.lower():
            organization = organization.replace(
                "Ltd", r"(Limited|Limted|Ltd)[.]{0,1}")
        if "holdings" in organization.lower():
            organization = organization.replace("Holdings", r"Holding[s]{0,1}")
        elif "holding" in organization.lower():
            organization = organization.replace("Holding", r"Holding[s]{0,1}")
        organization = organization.replace("+", r"[\+]{0,2}")
        organization = organization.replace("*", r"[\*]{0,2}")
        organization = organization.replace("(", r"[(]{0,2}")
        organization = organization.replace(")", r"[)]{0,2}")
        organization = organization.replace("-", r"[-]{0,2}")
        organization = organization.replace("#", r"[#]{0,2}")

        organizationList = organization.split(" ")
        organizationRegex = r"[\s]{0,3}".join(organizationList)
        if organizationId != "":
            organizationRegex2 = r"("+organizationRegex+r"|"+organizationId+")"
        else:
            organizationRegex2 = organizationRegex
        if "cotai" not in organization.lower():
            organizationRegex2 = r"(?!cotai)"+organizationRegex2
        organizationInDescRegex = r"(?i)[^\n]*" + \
            organizationRegex2+r"[^\n]*"

        organizationRegex3 = r"(?i)Supervisory Organization[/\s]{1,4}Enable for[:：\s]{0,4}[^\n]*" + organizationRegex2 +\
            r"[^\n]*"
    else:
        organizationRegex3 = organization
        organizationInDescRegex = organization
    requestActionRegex = r"(?i)Requested Action[:：\s]{1,4}(Add|新增)"
    temporaryAccesRegex = r"(?i)Temporary Access[:：\s]{1,4}(false|否)"
    expirationDateRegex = r"Expiration Date[:：\s]{1,4}[\d]{2}/[\d]{2}/[\d]{4}"
    # temporaryAccesRegex = r"(?i)Temporary Access[:：\s]{1,4}"
    businessEntityAry = businessEntity.split(" ")
    isDigitFlag = businessEntityAry[0].isdigit()
    if isDigitFlag:
        businessEntityRegex = r"(?i)Business Entity[:：\s]{1,4}" + \
            businessEntityAry[0]
    else:
        businessEntityRegex = r"(?i)Business Entity[:：\s]{1,4}"+businessEntity

    def exceptionSeach():
        #########################
        # handle exception ticket
        #########################
        foundTicketDf1 = ticketEptDf.loc[(ticketEptDf["Description"].str.contains(roleNameSearchRegex)) &
            (ticketEptDf["Description"].str.contains(requestActionRegex)) &
            ((ticketEptDf["Description"].str.contains(expirationDateRegex)) |
            (ticketEptDf["Description"].str.contains(temporaryAccesRegex))) &
            (ticketEptDf["User/Employee ID"].str.contains(empId))
            &
            (ticketEptDf["Description"].str.contains(
                appNameRegex))
                                         ]
        foundTicketDf2 = ticketEptDf.loc[
            (ticketEptDf["Description"].str.contains(roleNameSearchRegex)) &
            (ticketEptDf["Description"].str.contains(requestActionRegex)) &
            ((ticketEptDf["Description"].str.contains(expirationDateRegex)) |
             (ticketEptDf["Description"].str.contains(temporaryAccesRegex))) &
            (ticketEptDf["Description"].str.contains(empId))
            &
            (ticketEptDf["Description"].str.contains(appNameRegex))]
        foundTicketDf1Org = foundTicketDf1.loc[(
            foundTicketDf1["Description"].str.contains(organizationRegex3))]
        foundTicketDf1OrgInDesc = foundTicketDf1.loc[(
            foundTicketDf1["Description"].str.contains(organizationInDescRegex))]
        foundTicketDf2Org = foundTicketDf2.loc[(
            foundTicketDf2["Description"].str.contains(organizationRegex3))]
        foundTicketDf2OrgInDesc = foundTicketDf2.loc[(
            foundTicketDf2["Description"].str.contains(organizationInDescRegex))]
        if foundTicketDf1Org.shape[0] > 0:
            max1Org = foundTicketDf1Org["Number"].max()
        else:
            max1Org = "AAAA"
        if foundTicketDf2Org.shape[0] > 0:
            max2Org = foundTicketDf2Org["Number"].max()
        else:
            max2Org = "AAAA"
        if foundTicketDf1OrgInDesc.shape[0] > 0:
            max1OrgInDesc = foundTicketDf1OrgInDesc["Number"].max()
        else:
            max1OrgInDesc = "AAAA"
        if foundTicketDf2OrgInDesc.shape[0] > 0:
            max2OrgInDesc = foundTicketDf2OrgInDesc["Number"].max()
        else:
            max2OrgInDesc = "AAAA"
        if foundTicketDf1.shape[0] > 0:
            max1 = foundTicketDf1["Number"].max()
        else:
            max1 = "AAAA"
        if foundTicketDf2.shape[0] > 0:
            max2 = foundTicketDf2["Number"].max()
        else:
            max2 = "AAAA"

        maxNum = max(max1, max2)
        maxNumOrg = max(max1Org, max2Org)
        maxNumOrgInDesc = max(max1OrgInDesc, max2OrgInDesc)

        if str(empId) in printEmplyeeId:
            print("----- unbound loop ----")
            print("----- emyid=%s" % empId)

            print("---rolename2=%s" % roleName2)
            print("----- roleName=%s" % roleNameSearchRegex)
            print("----- org=%s" % organizationRegex3)
            print("----- orgIndesc=%s" % organizationInDescRegex)
            # print("BR",type(maxBR1),type(maxBR2),type(maxBR1Org),type(maxBR2Org))
            print("EA", type(max1), type(max2), type(max1Org), type(max2Org))
            print(foundTicketDf1Org[["Description"]])
            print(foundTicketDf2Org[["Description"]])
            print(foundTicketDf1[["Description"]], foundTicketDf1.shape[0])
            print(foundTicketDf2[["Description"]])
            # print(foundBRTicketDf1)
            # print(foundBRTicketDf2)
            print(maxNumOrg, max2OrgInDesc, maxNumOrgInDesc, maxNum)
        return maxNumOrg, maxNumOrgInDesc, maxNum

    def BRsearch():
        #########################
        # handle BR ticket
        #########################
        foundBRTicketDf1 = ticketBRDf.loc[(ticketBRDf["Description"].str
                                           .contains(roleNameSearchRegex)) &
                                          (ticketBRDf["User/Employee ID"].str.contains(empId))
                                          ]
        foundBRTicketDf2 = ticketBRDf.loc[
            (ticketBRDf["Description"].str.contains(roleNameSearchRegex)) &
            (ticketBRDf["Description"].str.contains(empId))]
        foundBRTicketDf1Org = foundBRTicketDf1.loc[(
            foundBRTicketDf1["Description"].str.contains(organizationRegex3))]
        foundBRTicketDf2Org = foundBRTicketDf2.loc[(
            foundBRTicketDf2["Description"].str.contains(organizationRegex3))]
        foundBRTicketDf1OrgInDesc = foundBRTicketDf1.loc[(
            foundBRTicketDf1["Description"].str.contains(organizationInDescRegex))]
        foundBRTicketDf2OrgInDesc = foundBRTicketDf2.loc[(
            foundBRTicketDf2["Description"].str.contains(organizationInDescRegex))]

        if foundBRTicketDf1Org.shape[0] > 0:
            maxBR1Org = foundBRTicketDf1Org["Number"].max()
        else:
            maxBR1Org = "AAAA"
        if foundBRTicketDf2Org.shape[0] > 0:
            maxBR2Org = foundBRTicketDf2Org["Number"].max()
        else:
            maxBR2Org = "AAAA"
        if foundBRTicketDf1OrgInDesc.shape[0] > 0:
            maxBR1OrgInDesc = foundBRTicketDf1OrgInDesc["Number"].max()
        else:
            maxBR1OrgInDesc = "AAAA"
        if foundBRTicketDf2OrgInDesc.shape[0] > 0:
            maxBR2OrgInDesc = foundBRTicketDf2OrgInDesc["Number"].max()
        else:
            maxBR2OrgInDesc = "AAAA"
        if foundBRTicketDf1.shape[0] > 0:
            maxBR1 = foundBRTicketDf1["Number"].max()
        else:
            maxBR1 = "AAAA"
        if foundBRTicketDf2.shape[0] > 0:
            maxBR2 = foundBRTicketDf2["Number"].max()
        else:
            maxBR2 = "AAAA"
        maxBRNum = max(maxBR1, maxBR2)
        maxBRNumOrg = max(maxBR1Org, maxBR2Org)
        maxBRNumOrgInDesc = max(maxBR1OrgInDesc, maxBR2OrgInDesc)
        return maxBRNumOrg, maxBRNumOrgInDesc, maxBRNum
    maxNumOrg, maxNumOrgInDesc, maxNum = exceptionSeach()

    maxBRNumOrg, maxBRNumOrgInDesc, maxBRNum = BRsearch()

    comment7 = ""
    complianceCheck9 = ""
    remark8 = ""
    expirationDate = ""
    resultNum = ""
    FoundFlag = False
    if maxNumOrg != "AAAA":
        comment7 = maxNumOrg
        remark8 = "new check"
        tempSubDf = ticketDf.loc[ticketDf["Number"] == comment7]
        desc = tempSubDf.loc[tempSubDf.index[0], "Description"]

        if re.search(r"(?i)Temporary Access[:：\s]{1,4}(false|否)", desc) is not None:
            expirationDate = ""
        else:
            reGroup = re.search(
                r"Expiration (Access|Date)[\s:]{1,4}[^\n]*\n", desc)
            begIdx, endIdx = reGroup.span()
            expirationDate = desc[begIdx:endIdx]
        complianceCheck9 = expirationDate+" ticket confirmed"
    elif maxNumOrg == "AAAA" and maxNumOrgInDesc != "AAAA":
        comment7 = maxNumOrgInDesc
        remark8 = "new check, org in desc"
        tempSubDf = ticketDf.loc[ticketDf["Number"] == comment7]
        desc = tempSubDf.loc[tempSubDf.index[0], "Description"]

        if re.search(r"(?i)Temporary Access[:：\s]{1,4}(false|否)", desc) is not None:
            expirationDate = ""
        else:
            reGroup = re.search(
                r"Expiration (Access|Date)[\s:]{1,4}[^\n]*\n", desc)
            begIdx, endIdx = reGroup.span()
            expirationDate = desc[begIdx:endIdx]
        complianceCheck9 = expirationDate+" ticket confirmed"
        complianceCheck9 = expirationDate+" ticket confirmed"
    elif maxNumOrg == "AAAA" and maxNumOrgInDesc == "AAAA" and maxNum != "AAAA":
        comment7 = maxNum
        remark8 = "new check, but no org confirmed"
        tempSubDf = ticketDf.loc[ticketDf["Number"] == comment7]
        desc = tempSubDf.loc[tempSubDf.index[0], "Description"]

        if re.search(r"(?i)Temporary Access[:：\s]{1,4}(false|否)", desc) is not None:
            expirationDate = ""
        else:
            reGroup = re.search(
                r"Expiration (Access|Date)[\s:]{1,4}[^\n]*\n", desc)
            begIdx, endIdx = reGroup.span()
            expirationDate = desc[begIdx:endIdx]
        complianceCheck9 = expirationDate+" ticket confirmed"
        complianceCheck9 = expirationDate+" ticket confirmed"
    else:
        if maxBRNumOrg != "AAAA":
            comment7 = maxBRNumOrg
            complianceCheck9 = "ticket confirmed"
            remark8 = "new check"
        elif maxBRNumOrg == "AAAA" and maxBRNumOrgInDesc != "AAAA":
            comment7 = maxBRNumOrgInDesc
            complianceCheck9 = "ticket confirmed"
            remark8 = "new check, org in desc"
        elif maxBRNumOrg == "AAAA" and maxBRNum != "AAAA":
            comment7 = maxBRNum
            complianceCheck9 = "ticket confirmed"
            remark8 = "new check, but no org confirmed"

    if comment7 != "":
        FoundFlag = True
        resultDf.loc[i, "Comment"] = comment7
        resultDf.loc[i, "Compliance Checking"] = complianceCheck9
        if pd.isna(resultDf.loc[i, "Remarks"]) or resultDf.loc[i, "Remarks"] in ["", "#N/A"]:
            resultDf.loc[i, "Remarks"] = remark8
        else:
            resultDf.loc[i, "Remarks"] = remark8+resultDf.loc[i, "Remarks"]
    return resultDf, FoundFlag


def descLooseSearch(resultDf, i, ticketDf, roleName, empId, organization, organizationId, businessEntity):
    ticketBRDf = ticketDf.loc[ticketDf["Item"] == "Birthright Access"]
    ticketEptDf = ticketDf.loc[ticketDf["Item"] == "Exception Access"]

    # if Remote Access for Workday Desktop View = Selective IP Unrestricted
    # if "Selective IP Unrestricted".lower() in roleName.lower():
    #     roleName2=roleName.lower().replace("Selective IP Unrestricted".lower(),"Remote Access".lower() )
    # else:
    appName = resultDf.loc[resultDf.index[0], "Application"]
    if "Cotai".upper() not in appName.upper():
        appNameRegex = r"(?i)(Application Name|應用程式名稱)[^\n]*[:,]{1}[\s]{0,4}((?!Cotai) %s)[\s]{0,2}" % appName
    else:
        appNameRegex = r"(?i)(Application Name|應用程式名稱)[^\n]*[:,]{1}[\s]{0,4}"+appName
    roleName2 = roleName

    roleNameList = roleName2.split(" ")
    roleNameSearchRegex = r"[\s]{0,3}".join(roleNameList)
    roleNameSearchRegex = r"(?i)"+roleNameSearchRegex
    if organization != "":
        if "limted" in organization.lower():
            organization = organization.replace(
                "Limted", r"(Limited|Limted|Ltd)[.]{0,1}")
        elif "limited" in organization.lower():
            organization = organization.replace(
                "Limited", r"(Limited|Limted|Ltd)[.]{0,1}")
        elif "ltd" in organization.lower():
            organization = organization.replace(
                "Ltd", r"(Limited|Limted|Ltd)[.]{0,1}")
        if "holdings" in organization.lower():
            organization = organization.replace(
                "Holdings", r"(Holiday|Holidays)[.]{0,1}")
        elif "holding" in organization.lower():
            organization = organization.replace(
                "Holding", r"(Holiday|Holidays)[.]{0,1}")
        organization = organization.replace(",", r"[,]{0,2}")
        organization = organization.replace("(", r"[(]{0,2}")
        organization = organization.replace(")", r"[)]{0,2}")
        organization = organization.replace("-", r"[-]{0,2}")
        organization = organization.replace("#", r"[#]{0,2}")

        organizationList = organization.split(" ")
        organizationRegex = r"[\s]{0,3}".join(organizationList)
        if organizationId != "":
            organizationRegex2 = r"("+organizationRegex+r"|"+organizationId+")"
        else:
            organizationRegex2 = organizationRegex
        if "cotai" not in organization.lower():
            organizationRegex2 = r"(?!cotai)"+organizationRegex2

        organizationRegex3 = r"(?i)[^\n]*"+organizationRegex2 +\
            r"[^\n]*"
    else:
        organizationRegex3 = organization
    requestActionRegex = r"(?i)Requested Action[:：\s]{1,4}(Add|新增)"
    temporaryAccesRegex = r"(?i)Temporary Access[:：\s]{1,4}(false|否)"
    businessEntityAry = businessEntity.split(" ")
    isDigitFlag = businessEntityAry[0].isdigit()
    if isDigitFlag:
        businessEntityRegex = r"(?i)Business Entity[:：\s]{1,4}" + \
            businessEntityAry[0]
    else:
        businessEntityRegex = r"(?i)Business Entity[:：\s]{1,4}"+businessEntity
    if "Selective IP Unrestricted".lower() in roleName.lower():
        ticketRADf = ticketDf.loc[ticketDf["Item"] ==
                                  "Remote Access for Workday Desktop View"]
        foundRATicketDf1 = ticketRADf.loc[
            (ticketRADf["Description"].str.contains(empId))
        ]
        foundRATicketDf2 = ticketRADf.loc[
            (ticketRADf["User/Employee ID"].str.contains(empId))
        ]
        maxRANum = "AAAA"
        print(foundRATicketDf1["Number"].max(),
              foundRATicketDf2["Number"].max())

        if foundRATicketDf1.shape[0] > 0:
            maxRA1 = foundRATicketDf1["Number"].max()
        else:
            maxRA1 = "AAAA"
        if foundRATicketDf2.shape[0] > 0:
            maxRA2 = foundRATicketDf2["Number"].max()
        else:
            maxRA2 = "AAAA"
        maxRANum = max(maxRA1, maxRA2)

        if maxRANum != "AAAA":
            resultDf.loc[i, "Comment"] = maxRANum
            resultDf.loc[i, "Compliance Checking"] = "confirmed ticket"
            FoundFlag = True
            if pd.isna(resultDf.loc[i, "Remarks"]) or resultDf.loc[i, "Remarks"] in ["", "#N/A"]:
                resultDf.loc[i, "Remarks"] = "new check, Remote Access for Workday Desktop View"
            else:
                resultDf.loc[i, "Remarks"] = "new check,Remote Access for Workday Desktop View " + \
                    resultDf.loc[i, "Remarks"]
            return resultDf, FoundFlag

        else:
            return resultDf, False

    def exceptionSeach():
        #########################
        # handle exception ticket
        #########################
        foundTicketDf1 = ticketEptDf.loc[(ticketEptDf["Description"].str
                                          .contains(roleNameSearchRegex)) &
                                         (ticketEptDf["Description"].str.contains(requestActionRegex)) &
                                         (ticketEptDf["Description"].str.contains(temporaryAccesRegex)) &
                                         (ticketEptDf["User/Employee ID"].str.contains(empId))
                                         &
                                         (
            (ticketEptDf["Description"].str.contains(appNameRegex))
        )
        ]
        foundTicketDf2 = ticketEptDf.loc[
            (ticketEptDf["Description"].str.contains(roleNameSearchRegex)) &
            (ticketEptDf["Description"].str.contains(requestActionRegex)) &
            (ticketEptDf["Description"].str.contains(temporaryAccesRegex)) &
            (ticketEptDf["Description"].str.contains(empId)) &
            (
                (ticketEptDf["Description"].str.contains(appNameRegex))
            )]
        foundTicketDf1Org = foundTicketDf1.loc[(
            foundTicketDf1["Description"].str.contains(organizationRegex3))]
        foundTicketDf2Org = foundTicketDf2.loc[(
            foundTicketDf2["Description"].str.contains(organizationRegex3))]
        if foundTicketDf1Org.shape[0] > 0:
            max1Org = foundTicketDf1Org["Number"].max()
        else:
            max1Org = "AAAA"
        if foundTicketDf2Org.shape[0] > 0:
            max2Org = foundTicketDf2Org["Number"].max()
        else:
            max2Org = "AAAA"
        if foundTicketDf1.shape[0] > 0:
            max1 = foundTicketDf1["Number"].max()
        else:
            max1 = "AAAA"
        if foundTicketDf2.shape[0] > 0:
            max2 = foundTicketDf2["Number"].max()
        else:
            max2 = "AAAA"

        maxNum = max(max1, max2)
        maxNumOrg = max(max1Org, max2Org)
        if str(empId) in printEmplyeeId:
            print("----- Loose unbound loop ----")
            print("----- emyid=%s" % empId)
            print("----- roleName=%s" % roleNameSearchRegex)
            print("----- org=%s" % organizationRegex3)
            # print("BR",type(maxBR1),type(maxBR2),type(maxBR1Org),type(maxBR2Org))
            print("EA", type(max1), type(max2), type(max1Org), type(max2Org))
            print(foundTicketDf2[["Description"]])
            print(foundTicketDf2Org[["Description"]])
            # print(foundBRTicketDf1)
            # print(foundBRTicketDf2)
            print(max1Org, max2Org, maxNumOrg, max1, max2)
        return maxNumOrg, maxNum

    def BRsearch():
        #########################
        # handle BR ticket
        #########################
        foundBRTicketDf1 = ticketBRDf.loc[(ticketBRDf["Description"].str
                                           .contains(roleNameSearchRegex)) &
                                          (ticketBRDf["User/Employee ID"].str.contains(empId))
                                          ]
        foundBRTicketDf2 = ticketBRDf.loc[
            (ticketBRDf["Description"].str.contains(roleNameSearchRegex)) &
            (ticketBRDf["Description"].str.contains(empId))]
        foundBRTicketDf1Org = foundBRTicketDf1.loc[(
            foundBRTicketDf1["Description"].str.contains(organizationRegex3))]
        foundBRTicketDf2Org = foundBRTicketDf2.loc[(
            foundBRTicketDf2["Description"].str.contains(organizationRegex3))]
        if foundBRTicketDf1Org.shape[0] > 0:
            maxBR1Org = foundBRTicketDf1Org["Number"].max()
        else:
            maxBR1Org = "AAAA"
        if foundBRTicketDf2Org.shape[0] > 0:
            maxBR2Org = str(foundBRTicketDf2Org["Number"].max())
        else:
            maxBR2Org = "AAAA"
        if foundBRTicketDf1.shape[0] > 0:
            maxBR1 = foundBRTicketDf1["Number"].max()
        else:
            maxBR1 = "AAAA"
        if foundBRTicketDf2.shape[0] > 0:
            maxBR2 = foundBRTicketDf2["Number"].max()
        else:
            maxBR2 = "AAAA"
        maxBRNum = max(maxBR1, maxBR2)
        maxBRNumOrg = max(maxBR1Org, maxBR2Org)
        return maxBRNumOrg, maxBRNum

    maxNumOrg, maxNum = exceptionSeach()

    maxBRNumOrg, maxBRNum = BRsearch()

    comment7 = ""
    complianceCheck9 = ""
    remark8 = ""

    if maxNumOrg != "AAAA":
        comment7 = maxNumOrg
        complianceCheck9 = "ticket confirmed"
        remark8 = "new check, in description"
    elif maxNumOrg == "AAAA" and maxNum != "AAAA":
        comment7 = maxNum
        complianceCheck9 = "ticket confirmed"
        remark8 = "new check, in description, but no org confirmed"
    else:
        if maxBRNumOrg != "AAAA":
            comment7 = maxBRNumOrg
            complianceCheck9 = "ticket confirmed"
            remark8 = "new check, in description"
        elif maxBRNumOrg == "AAAA" and maxBRNum != "AAAA":
            comment7 = maxBRNum
            complianceCheck9 = "ticket confirmed"
            remark8 = "new check, in description, but no org confirmed"

    FoundFlag = False
    if comment7 != "":
        resultDf.loc[i, "Comment"] = comment7
        resultDf.loc[i, "Compliance Checking"] = complianceCheck9
        FoundFlag = True
        if pd.isna(resultDf.loc[i, "Remarks"]) or resultDf.loc[i, "Remarks"] in ["", "#N/A"]:
            resultDf.loc[i, "Remarks"] = remark8
        else:
            resultDf.loc[i, "Remarks"] = remark8+resultDf.loc[i, "Remarks"]
    return resultDf, FoundFlag
def concatIEERoleIdInfo(b, d, f, j, k, l):
    if j in ["#N/A", np.nan, ""]:
        if b not in ["#N/A", np.nan, ""]:
            return str(b).strip()+str(d).strip()+str(f).strip()
        else:
            if int(b) > 50000:
                return str(b).strip()+str(d).strip()+str(f).strip()
            else:
                return ""
    else:
        return str(b).strip()+str(d).strip()+str(f).strip()+str(j).strip()+str(k).strip()+str(l).strip()


def concatIEEBasedIdInfo(b, e, i, j, k):
    if i in ["#N/A", np.nan, ""]:
        if b not in ["#N/A", np.nan, ""]:
            return str(b).strip()+str(e).strip()
        else:
            if int(b) > 50000:
                return str(b).strip()+str(e).strip()
                return ""
    else:
        return str(b).strip()+str(i).strip()+str(j).strip()+str(e).strip()+str(k).strip()


def comment7BasedFun(companyCode, beChecking, ccChecking, businessEntity, lookUpVal):
    if lookUpVal not in ["#N/A", np.nan, ""]:
        return lookUpVal
    else:
        if ccChecking == "ALL" and beChecking == "ALL":
            return "BR"
        elif ccChecking == "ALL" and beChecking == businessEntity:
            return "BR"
        elif ccChecking == companyCode and beChecking == "ALL":
            return "BR"


def basedGroupCheck(folderPath, prefixBaseName="based_result",
                    checkColnHead=["Employee ID #", "Security Group"], appName="Workday"):
    # , skiprows=[0,1,2,3,4]
    ##############
    # define Path
    ##########
    IEEPathAry = glob.glob(folderPath+"/INT030_EE_User_Based_Group*")
    if len(IEEPathAry) > 0:
        IEEPath = IEEPathAry[0]
    brPathAry = glob.glob(
        folderPath+"/BirthRight_Role_Entitlement_-_Weekly_Report*")
    if len(brPathAry) > 0:
        brPath = brPathAry[0]

    activeEmplyeeListPathAry = glob.glob(
        folderPath+"/MGM IT - Active Employee List*")
    if len(activeEmplyeeListPathAry) > 0:
        activeEmplyeeListPath = activeEmplyeeListPathAry[0]
    ticketPathAry = glob.glob(folderPath+"/sc_*.xlsx")
    if len(ticketPathAry) > 0:
        ticketPath = ticketPathAry[0]
    based1PathAry = glob.glob(folderPath+"/based_result*")
    if len(based1PathAry) > 0:
        based1Path = based1PathAry[0]

    ####################################
    # get active Employee Data
    ###################################
    # activeDf = pd.read_csv(activeEmplyeeListPath, dtype=str, skiprows=[0])
    activeDf = pd.DataFrame(
        pd.read_excel(activeEmplyeeListPath, dtype=str, skiprows=[0, 1]
                      ))
    # Emp ID
    # Business Entity,Company Code
    activeDf.rename(columns={'Emp ID': 'Employee ID #', "Company - ID": "Company Code", "Division ID": "DIVISION CODE",
                    "Section ID": "SECTION CODE", "Department ID": "DEPARTMENT CODE"}, inplace=True)

    activeDf["Preferred Name"] = activeDf["Preferred Name"] + \
        " "+activeDf["Last Name"]

    activeSubDf = activeDf[['Employee ID #', "Preferred Name", "Job Profile Code",
                            "Business Entity", "Company Code", "DIVISION CODE", "DEPARTMENT CODE", "SECTION CODE"]]

    # print("===activeDF===")
    # print(activeSubDf.head())
    ######################
    # company code =001----> (int)1----> (str)1
    #####################
    activeSubDf["Company Code"] = activeSubDf["Company Code"].astype(
        int).astype(str)

    birthRightDf = pd.read_csv(brPath, dtype=str)
    birthRightDf["Applicationname"] = birthRightDf["Applicationname"].astype(str)
    birthRightDf["Job Profile_Security Group"] = birthRightDf["Job Profile"] + \
        birthRightDf["Entitlement"]
    birthRightDf["Job Profile_Business Entity_Company Code_Entitlement"] = birthRightDf["Job Profile"] + \
        birthRightDf["Business Entity"] + \
        birthRightDf["Company Code"]+birthRightDf["Entitlement"]

    ####################################
    # get IEE role assignment data
    ###################################
    # IEEDf = pd.read_csv(IEEPath, dtype=str)
    IEEDf = pd.DataFrame(
        pd.read_excel(IEEPath, dtype=str, skiprows=[0, 1, 2]))
    ieeHeader = IEEDf.columns

    ####################################
    # exclude the required role name
    ###################################

    ####################################
    # get last  role checked assignment data
    ###################################
    based1Df = pd.DataFrame(pd.read_excel(based1Path, dtype=str))
    ####################################
    # clean up the last "new check" from Remark8
    ###################################
    lastCheckedBasedDf = based1Df.loc[based1Df["Remarks"] == "new check"]
    based1Df.loc[lastCheckedBasedDf.index, "Remarks"] = ""
    basedHeader = based1Df.columns
    based2Df = pd.DataFrame(dtype=str)
    # role1Df=pd.read_csv(folderPath+"/WD_role_assignment1.csv")
    # role2Df=pd.read_csv(folderPath+"/WD_role_assignment2.csv")
    # ticketDf=pd.read_csv(folderPath+"/ticket.csv")
    ####################################
    # get ticket data
    ###################################
    ticketDf = pd.DataFrame(pd.read_excel(ticketPath, dtype=str))
    # WD_role_assignment1
    # BirthRight_Role_Entitlement_-_Weekly_Report
    # INT030_EE_Role_Assignment

    ###################################################
    # begin to generate the new checked role assignment data
    ##################################################
    based2Df[basedHeader[1]] = IEEDf[ieeHeader[0]].astype(
        str)  # Employee ID #
    
    # Role Name,Organization,Organization_Reference_ID
    based2Df[basedHeader[2]] = IEEDf[ieeHeader[5]].astype(
        str)  # Business Title
    based2Df[basedHeader[3]] = IEEDf[ieeHeader[6]].astype(
        str)  # INTShare-Management-Level
    based2Df[basedHeader[4]] = IEEDf[ieeHeader[7]].astype(
        str)  # organization_Reference_ID
    print("====based2====")
    print("inject iEE, shape", based2Df.shape)

    ###################################################
    # role3:
    # according employ ID, join acitve employee data and role2Data from IEE
    ##################################################
    based3Df = pd.merge(based2Df, activeSubDf, on='Employee ID #', how="left")
    based3Df[basedHeader[0]] = based3Df.apply(lambda row: concatIEEBasedIdInfo(
        row['Employee ID #'], row['Security Group'],
        row['Job Profile Code'],
        row['Business Entity'],
        row['Company Code']), axis=1)

    ###################################################
    # role3:
    # according employ ID, join acitve employee data and role2Data from IEE
    ##################################################
    based3Df["Job Profile_Security Group"] = "'" +\
        based3Df["Job Profile Code"] +\
        "'User-Based-Group : "+based3Df["Security Group"]
    print("merge active, shape", based3Df.shape)
    # based3Df["Application"]="Workday"

    based4Df = pd.merge(based3Df, birthRightDf[[
                        "Job Profile_Security Group", "Company Code", "Business Entity"]], on='Job Profile_Security Group', how="left")
    # print(based4Df[["Applicationname"]].head())
    based4Df.rename(columns={'Company Code_x': 'Company Code','Company Code_y': 'CC Checking(From BR)',"Business Entity_x": "Business Entity",
                             "Business Entity_y": "BE Checking(From BR)"}, inplace=True)

    print("merge last base, shape", based4Df.shape)
    print("==base4Df==")

    ###################################################
    # role4Df:
    # according the totalIdInfo(defomed by function concatInfo),
    #  join last checked role assignment data and role3Data from IEE
    ##################################################
    # companyCode, beChecking,ccChecking,businessEntity,
    based5Df = pd.merge(based4Df, based1Df[[
        basedHeader[0], 'Comment', 'Remarks', 'Compliance Checking']], on=basedHeader[0], how="left")
    based5Df["Comment"] = based5Df.apply(lambda row: comment7BasedFun(
        row['Company Code'], row['BE Checking(From BR)'],
        row['CC Checking(From BR)'], row['Business Entity'],
        row['Comment']), axis=1)
    # based5Df["Application"] = 
    print("==based5Df==")
    print(based5Df.columns)
    print(based5Df.head())

    ##################################################
    # unfoundDf:
    # handleing the NaN comment
    ##################################################
    based5Df.loc[:, 'Application'] = appName
    unfoundDf = based5Df.loc[(based5Df["Comment"].isna()) |
                             ((based5Df["Remarks"].str.contains("please provide supporting")) |
                              based5Df["Remarks"].isna())]
    # .isin(["","#N/A"])
    print("=====unfound====")
    print(unfoundDf)
    # print("unfoundDF  %s" % unfoundDf.shape[0])
    for i in unfoundDf.index:
        # 'Employee ID #'
        # roleName=unfoundDf.loc[i,"Role Name"]
        empId = unfoundDf.loc[i, 'Employee ID #']
        securityGroup = unfoundDf.loc[i, "Security Group"]
        # if "Selective IP Unrestricted".lower() in securityGroup.lower():
        #     roleName2 = securityGroup.lower().replace("Selective IP Unrestricted".lower(),"Remote Access".lower())
        # else:
        securityGroup2 = securityGroup
        securityGroup2 = securityGroup2.replace("(", r"\(")
        securityGroup2 = securityGroup2.replace(")", r"\)")
        securityGroupList = securityGroup2.split(" ")
        securityGroupSearchRegex = r"[\s]{1,3}".join(securityGroupList)
        securityGroupSearchRegex = r"(?i)" + securityGroupSearchRegex
        businessEntity = str(unfoundDf.loc[i, "Business Entity"])
        companyCode = str(unfoundDf.loc[i, "Company Code"])
        for j in range(0, 3-len(companyCode)):
            companyCode = "0"+companyCode
        print(companyCode)
        jobProfile = unfoundDf.loc[i, "Job Profile Code"]
        if len(str(jobProfile)) < 6:
            jobProfile = "0"+str(jobProfile)
        # organization = unfoundDf.loc[i, "Organization"]

        foundBRDf = birthRightDf.loc[
            (birthRightDf["Entitlement"].str.contains(
                securityGroupSearchRegex)) &
            (birthRightDf["Job Profile"].str.contains(jobProfile)) &
            (birthRightDf["Business Entity"].str
             .contains(r"(?i)" + businessEntity+r"|ALL")) &
            (birthRightDf["Company Code"].str
             .contains(r"(?i)" + companyCode+r"|ALL"))
        ]
        # print("---- foundBR %s %s %s----" %(empId,securityGroup2,jobProfile))
        # print(foundBRDf[["Job Profile","Business Entity","Company Code"]])

        # foundBRDf = birthRightDf.loc[\
        #     (birthRightDf["Entitlement"]str.cocntains(businessEntity)) &
        #                                 (birthRightDf["Job Profile"].str.contains(jobProfile))]
        # based5Df = based5Df.assign(Application=appName)
        # 
        if foundBRDf.shape[0] == 1:
            based5Df.loc[i, "Comment"] = "BR"
            based5Df.loc[i, "Compliance Checking"] = "BR confirmed"
            if pd.isna(based5Df.loc[i, "Remarks"]) or based5Df.loc[i, "Remarks"] in ["", "#N/A"]:
                based5Df.loc[i, "Remarks"] = "new check"
            else:
                based5Df.loc[i, "Remarks"] = "new check " + \
                    based5Df.loc[i, "Remarks"]
        elif foundBRDf.shape[0] == 0:
            based5Df, FoundFlag = descSearch(
                based5Df, i, ticketDf, securityGroup, empId, "", "", businessEntity)
            if FoundFlag:

                based5Df.loc[i, "Comment"] = "BR>1, need to check " + \
                    based5Df.loc[i, "Comment"]

            elif not FoundFlag:
                based5Df, FoundFlag = descLooseSearch(
                    based5Df, i, ticketDf, securityGroup, empId, "", "", businessEntity)
        else:

            based5Df, FoundFlag = descSearch(
                based5Df, i, ticketDf, securityGroup, empId, "", "", businessEntity)
            FoundFlag2 = None
            if not FoundFlag:
                based5Df, FoundFlag = descLooseSearch(
                    based5Df, i, ticketDf, securityGroup, empId, "", "", businessEntity)
            elif not FoundFlag:
                if FoundFlag2 is not None and not FoundFlag2:
                    based5Df.loc[i, "Comment"] = "BR>1, need to check " + \
                        based5Df.loc[i, "Comment"]
                elif FoundFlag2 is not None and not FoundFlag2:
                    based5Df.loc[i, "Comment"] = "BR>1, need to check"                
                    based5Df.loc[i, "Compliance Checking"] = ""
                    if pd.isna(based5Df.loc[i, "Remarks"]) or based5Df.loc[i, "Remarks"] in ["", "#N/A"]:
                        based5Df.loc[i, "Remarks"] = "new check"
                    else:
                        based5Df.loc[i, "Remarks"] = "new check " + \
                            based5Df.loc[i, "Remarks"]
                    based5Df.loc[i, "Compliance Checking"] = ""
    ##################################################
    # reorder the columns for output  #
    ##################################################
    based5Df = based5Df[
        [basedHeader[0], "Employee ID #", "Business Title",

         "INTShare-Management-Level", "Security Group",
            'Comment', 'Remarks', 'Compliance Checking',
         "Job Profile Code", "Business Entity", "Company Code",
         "BE Checking(From BR)", "CC Checking(From BR)", "DIVISION CODE", "DEPARTMENT CODE", "SECTION CODE","Job Profile_Security Group","Application"
         ]
    ]
    # print(role4Df.loc[role4Df["Employee ID #"]=="103025"])
    # based5Df.to_csv("based.csv", index=False)
    writer = pd.ExcelWriter(folderPath+"/based_result_%s.xlsx" %
                            dateExcelString, engine='xlsxwriter')
    based5Df.to_excel(writer, index=False, sheet_name='%s-EC' %
                      dateExcelString)

    workbook = writer.book
    worksheet = writer.sheets['%s-EC' %
                              dateExcelString]

    # adjust the column widths based on the content
    widthList = [23.21, 11.86, 21.57, 13.79, 28.36,
                 14.5, 14.5, 14.5, 8.5, 8.5, 8.5, 8.5, 8.5,  8.5,8.5,8.5,23.36,8.5]
    wrap_format = workbook.add_format({'text_wrap': True})
    for i, col in enumerate(based5Df.columns):
        # width = max(df[col].apply(lambda x: len(str(x))).max(), len(col))
        worksheet.set_column(i, i, widthList[i], wrap_format)
        worksheet

    my_formats = {
        'Employee ID #': "#C0E6F5",
        'Business Title': "#C0E6F5",
        'INTShare-Management-Level': "#C0E6F5",
        'Security Group': "#C0E6F5",
        'Job Profile Code': "#FBE2D5",
        'Business Entity': "#FBE2D5",
        'Company Code': "#FBE2D5",
        'Job Profile_Security Group': "#F1A983",
        'CC Checking(From BR)': "#94DCF8",
        'BE Checking(From BR)': '#94DCF8',
        "Comment": "#92D050",
        "Remarks": "#92D050",
        "Compliance Checking": "#92D050",
        'DIVISION CODE': "#CCC0DA",

        'DEPARTMENT CODE': "#CCC0DA",

        'SECTION CODE': "#CCC0DA",
        "Application": '#D9D9D9'
    }

    for col_num, value in enumerate(based5Df.columns.values):
        if col_num in [0]:
            print(col_num, value)
            format1 = workbook.add_format({'bg_color': "#C0E6F5"})
            worksheet.write(0, col_num, dateFmt_String, format1)
        elif value in my_formats.keys():
            format1 = workbook.add_format({'bg_color': my_formats[value]})
            worksheet.write(0, col_num, value, format1)
        else:
            format1 = workbook.add_format({})
            worksheet.write(0, col_num, value, format1)
    (max_row, max_col) = based5Df.shape
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    # save the Excel file
    try:
        writer._save()
    except:
        writer.save()
    return folderPath+"/based_result_%s.xlsx" %dateExcelString

workdir="/var/www/html/code/"

if not os.path.exists(workdir):
    workdir=""
if len(sys.argv) == 3:
    if sys.argv[1][0] == "2":
        routeName = sys.argv[2]
        dateRun = sys.argv[1]
    else:
        routeName = sys.argv[1]
        dateRun = sys.argv[2]
else:
    routeName = "role"
    dateRun = "20240813"
    # dateRun=dateString
if (os.path.exists(workdir+dateRun+"/"+dateRun)):
    print("path exists")
    folderPath = workdir+dateRun+"/"+dateRun
else:
    folderPath = workdir+dateRun
print(folderPath)
if routeName.lower() == "role" and os.path.exists(folderPath):
    fileName=roleCheck(folderPath)
elif routeName.lower() in ["base", "based", "basegroup", "basedgroup"] and \
        os.path.exists(folderPath):
    fileName=basedGroupCheck(folderPath)
else:
    fileName=""

# f = open(, "a+")
if os.path.exists(workdir+"output.txt"):
    with open(workdir+"output.txt", 'r') as f1:
        text = f1.read()
    f1.close()
    with open(workdir+"output.txt", 'a') as f:
        
        print("--------------------")
        print(text)
        if fileName not in text:
            f.write(fileName+"\n")

