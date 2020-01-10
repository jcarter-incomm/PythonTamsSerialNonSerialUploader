import pandas as pd
import os
from os import path
import pyodbc
import glob
from tkinter import *
from tkinter import filedialog
from Item import Item
from NonItem import NonItem
###################################################################################################
# @author: Nik Jenson                                                                             #
# Date: 12/31/2019                                                                                #
###################################################################################################
###################################################################################################
# Database Connection information.
# This will need to be switched between dev and local db
conn = pyodbc.connect('Driver={SQL Server};'  
                      'Server=(local)\SQLEXPRESS;'  
                      'Database=WMSDB;'  
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
###################################################################################################
#File write output
locationFile = open("C:\Tools\sqlChangeLogs\LocationFile.txt", "a+")
skuFile = open("C:\Tools\sqlChangeLogs\SkuFile.txt", "a+")
itemSerializedFile = open("C:\Tools\sqlChangeLogs\ItemSerializedFile.txt", "a+")
itemNonSerializedFile = open("C:\Tools\sqlChangeLogs\ItemNonSerializedFile.txt", "a+")
###################################################################################################
#Global variables
nonItemCount = 0
itemCount = 0
locationCount = 0
skuCount = 0
locationChange = False
skuChange = False
quantityChange = False
###################################################################################################
def dbItemNonSerialized(dbLocationID, dbSkuID, x):
    try:
        global locationChange
        global skuChange
        global quantityChange
        dbLocationID = int(dbLocationID)
        nonItemQuantity = cleanser(x.quantity)
        intQuantity = int(nonItemQuantity)
        locationChange = False
        skuChange = False
        quantityChange = False
        dbSkuID = int(dbSkuID)
        compoundKeyDb = cursor.execute("SELECT id FROM itemNonSerialized WHERE skuId = ? AND locationId = ?",
            (dbSkuID,dbLocationID)).fetchone()
        if compoundKeyDb is None:
            cursor.execute("INSERT INTO itemNonSerialized(skuId, locationId, quantity) VALUES(?,?,?)",
                dbSkuID, dbLocationID, intQuantity)
            conn.commit()
            print('Insert into ItemNonSerialized successful')
            incrementItemNonSerializedChangeCounter()
            dbNewNonItemSerializedID = cursor.execute("SELECT id FROM itemNonSerialized WHERE skuId = ? AND locationId = ?",
                (dbSkuID,dbLocationID)).fetchone()
            itemNonSerializedFile.write(
                'inserted into itemNonSeralized: ' + cleanser(str(dbNewNonItemSerializedID)) + ' ' +  
                str(dbSkuID) + ' ' + str(dbLocationID) + ' ' + str(intQuantity) + ' \n')
        else:
            compoundKeyDb = cleanser(compoundKeyDb)
            compoundKeyDb = int(compoundKeyDb)
            dbNonSerializedItem = cursor.execute(
                    "SELECT id, skuId, locationId, quantity FROM itemNonSerialized WHERE id = ?",
                    (compoundKeyDb)).fetchone()
            skuNum = int(dbNonSerializedItem[1])
            dbNonLocationID = int(dbNonSerializedItem[2])
            quantityNum = int(dbNonSerializedItem[3])
            if skuNum != dbSkuID:
                    skuChange = True
                    cursor.execute("UPDATE itemNonSerialized SET skuId = ? WHERE id = ?", dbSkuID, compoundKeyDb)
                    conn.commit()
                    print('Item sku id was changed')
                    incrementItemNonSerializedChangeCounter()
            if dbNonLocationID != dbLocationID:
                    locationChange = True
                    cursor.execute("UPDATE itemNonSerialized SET locationId = ? WHERE id = ?", dbLocationID,
                                compoundKeyDb)
                    conn.commit()
                    print('Item location id was changed')
                    incrementItemNonSerializedChangeCounter()
            if quantityNum != intQuantity:
                    quantityChange = True
                    cursor.execute("UPDATE itemNonSerialized SET quantity = ? WHERE id = ?", intQuantity,
                                compoundKeyDb)
                    conn.commit()
                    print('Item quantity was changed')
                    incrementItemNonSerializedChangeCounter()
            if locationChange and skuChange and quantityChange == True:
                    itemNonSerializedFile.write(
                        'Updated location sku and quantity itemNonSeralized: ' + str(compoundKeyDb) + ' ' 
                            + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + str(nonItemQuantity) + ' \n'))
            elif locationChange == True:
                    itemNonSerializedFile.write(
                        'Updated location itemNonSeralized: ' + cleanser(str(compoundKeyDb)) + ' ' 
                            + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + str(nonItemQuantity) + ' \n'))
            elif skuChange == True:
                    itemNonSerializedFile.write(
                        'Updated sku itemNonSeralized: ' + cleanser(str(compoundKeyDb)) + ' ' 
                            + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + str(nonItemQuantity) + ' \n'))
            elif quantityChange == True:
                    itemNonSerializedFile.write(
                        'Updated quantity itemNonSeralized: ' + cleanser(str(compoundKeyDb)) + ' ' 
                            + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + str(nonItemQuantity) + ' \n'))
            else:
                    print('No changes necessary')
    except:
        print('error occured in function dbItemNonSerialized, here is the object details:' + ' sku: ' + str(x.sku)
              + ' description: ' + str(x.description) + ' quantity: ' + str(x.quantity) + ' location: ' + str(x.location))

def dbItemSerialized(dbLocationID, dbSkuID, x):
    try:
        global locationChange
        global skuChange
        locationChange = False
        skuChange = False
        itemSerialNumber = str(x.serial)
        dbItemSerializedID = cursor.execute("SELECT id FROM itemSerialized WHERE serialNumber = ?",
                                            ([itemSerialNumber])).fetchone()
        if dbItemSerializedID is None:
            cursor.execute("INSERT INTO itemSerialized(serialNumber, skuId, locationId) VALUES(?,?,?)",
                           itemSerialNumber, dbSkuID, dbLocationID)
            conn.commit()
            print('Insert into ItemSerialized successful')
            incrementItemSerializedChangeCounter()
            dbNewItemSerializedID = cursor.execute("SELECT id FROM itemSerialized WHERE serialNumber = ?",
                                                   ([itemSerialNumber])).fetchone()
            itemSerializedFile.write(
                'inserted into itemSeralized: ' + cleanser(str(dbNewItemSerializedID)) + ' ' + str(itemSerialNumber) + ' ' + str(
                    dbSkuID + ' ' + str(dbLocationID) + ' ' + 'NULL' + ' \n'))
        else:
            dbItemSerializedID = cleanser(dbItemSerializedID)
            dbSerializedItem = cursor.execute(
                "SELECT id, serialNumber, skuId, locationId, statusId FROM itemSerialized WHERE id = ?",
                ([dbItemSerializedID])).fetchone()
            skuNum = str(dbSerializedItem[2])
            serialNum = str(dbSerializedItem[3])
            if skuNum != dbSkuID:
                skuChange = True
                cursor.execute("UPDATE itemSerialized SET skuId = ? WHERE id = ?", dbSkuID, dbItemSerializedID)
                conn.commit()
                print('Item sku id was changed')
                incrementItemSerializedChangeCounter()
            if serialNum != dbLocationID:
                locationChange = True
                cursor.execute("UPDATE itemSerialized SET locationId = ? WHERE id = ?", dbLocationID,
                               dbItemSerializedID)
                conn.commit()
                print('Item location id was changed')
                incrementItemSerializedChangeCounter()
            #else:
            if locationChange and skuChange == True:
                itemSerializedFile.write(
                    'Updated location and sku itemSeralized: ' + str(dbItemSerializedID) + ' ' + str(
                        itemSerialNumber) + ' ' + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + 'NULL' + ' \n'))
            elif locationChange == True:
                itemSerializedFile.write(
                    'Updated location itemSeralized: ' + cleanser(str(dbItemSerializedID)) + ' ' + str(
                        itemSerialNumber) + ' ' + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + 'NULL' + ' \n'))
            elif skuChange == True:
                itemSerializedFile.write(
                    'Updated sku itemSeralized: ' + cleanser(str(dbItemSerializedID)) + ' ' + str(
                        itemSerialNumber) + ' ' + str(dbSkuID + ' ' + str(dbLocationID) + ' ' + 'NULL' + ' \n'))
            else:
                print('No changes necessary')
    except:
        print('error occured in function dbItemSerialized, here is the object details:' + ' sku: ' + str(x.sku)
              + ' description: ' + str(x.description) + ' serial: ' + str(x.serial) + ' location: ' + str(x.location))


def dbSku(x):
    try:
        skuID = x.sku
        skuDescription = x.description
        dbskuID = cursor.execute("SELECT id FROM productSku WHERE sku = ?", ([skuID])).fetchone()
        if hasattr(x, 'serial'):
            if dbskuID is None:
                cursor.execute("INSERT INTO productSku(sku, description, serialized) VALUES(?,?,?)", skuID, skuDescription,
                            '1')
                conn.commit()
                incrementskuChangeCounter()
                dbskuID = cursor.execute("SELECT id FROM productSku WHERE sku = ?", ([skuID])).fetchone()
                skuFile.write("productsku table id insert: " + cleanser(str(skuID)) + ' \n')
                return dbskuID
            else:
                return dbskuID
        else:
            if dbskuID is None:
                cursor.execute("INSERT INTO productSku(sku, description, serialized) VALUES(?,?,?)", skuID, skuDescription,
                            '0')
                conn.commit()
                incrementskuChangeCounter()
                dbskuID = cursor.execute("SELECT id FROM productSku WHERE sku = ?", ([skuID])).fetchone()
                skuFile.write("productsku table id insert: " + cleanser(str(skuID)) + ' \n')
                return dbskuID
            else:
                return dbskuID
    except:
        print('error occured in function dbSku, here is the object details:' + ' sku: ' + str(x.sku)
              + ' description: ' + str(x.description) +  ' location: ' + str(x.location))


def dbLocation(x):
    try:
        locationID = x.location
        dbLocationID = cursor.execute("SELECT id FROM location WHERE name = ?", ([locationID])).fetchone()
        if dbLocationID is None:
            cursor.execute("INSERT INTO location(name, warehouseNumber) VALUES(?,?)", locationID, '70')
            conn.commit()
            incrementlocationChangeCounter()
            dbLocationID = cursor.execute("SELECT id FROM location WHERE name = ?", ([locationID])).fetchone()
            locationFile.write("Location table id insert: " + cleanser(str(dbLocationID)) + ' \n')
            return dbLocationID
        else:
            return dbLocationID
    except:
        print('error occured in function dbLocation, here is the object details:' + ' sku: ' + str(x.sku)
              + ' description: ' + str(x.description) + ' serial: ' + ' location: ' + str(x.location))


def cleanser(string):
    makeString = str(string)
    stripPar = makeString.strip(',(),')
    replaceComma = stripPar.replace(',', '')
    removeWhiteSpace = replaceComma.strip()
    return removeWhiteSpace

#################################################################################################
def incrementItemNonSerializedChangeCounter():
    global nonItemCount
    nonItemCount = nonItemCount + 1

def incrementItemSerializedChangeCounter():
    global itemCount
    itemCount = itemCount + 1

def incrementlocationChangeCounter():
    global locationCount
    locationCount = locationCount + 1

def incrementskuChangeCounter():
    global skuCount
    skuCount = skuCount + 1

def getNonItemCounter():
    return nonItemCount

def getItemCounter():
    return itemCount

def getLocationCounter():
    return locationCount

def getSkuCounter():
    return skuCount
#####################################################################################################

def dbUploader(xls):
    itemList = []
    if 'SERIAL' in xls.columns:
        for index, row in xls.iterrows():
            item = Item(row['SKU'], row['DESCRIPTION'], row['SERIAL'], row['LOCATION'])
            itemList.append(item)

        if not itemList:
            print('##################################################')
            print("Objects could not be created from this excel sheet")
            main()
        else:
            count = 0
            for x in itemList:
                dbLocationID = dbLocation(x)
                dbSkuID = dbSku(x)
                dbItemSerialized(cleanser(dbLocationID), cleanser(dbSkuID), x)
                count = count + 1
                if count == len(itemList):
                    itemSerializedFile.write(
                        'Rows changed: ' + str(getItemCounter()) + ' \n')
                    skuFile.write(
                        'Rows changed: ' + str(getSkuCounter()) + ' \n')
                    locationFile.write(
                        'Rows changed: ' + str(getLocationCounter()) + ' \n')
                    print('End of file')
                    #initialize()
    else:
        for index, row in xls.iterrows():
            nonItem = NonItem(row['SKU'], row['DESCRIPTION'], row['LOCATION'], row['QUANTITY'])
            itemList.append(nonItem)
        if not itemList:
            print('##################################################')
            print("Objects could not be created from this excel sheet")
            main()
        else:
            count = 0
            for x in itemList:
                dbLocationID = dbLocation(x)
                dbSkuID = dbSku(x)
                dbItemNonSerialized(cleanser(dbLocationID), cleanser(dbSkuID), x)
                count = count + 1
                if count == len(itemList):
                    itemNonSerializedFile.write(
                        'Rows changed: ' + str(getNonItemCounter()) + ' \n')
                    skuFile.write(
                        'Rows changed: ' + str(getSkuCounter()) + ' \n')
                    locationFile.write(
                        'Rows changed: ' + str(getLocationCounter()) + ' \n')
                    print('End of file')
                    #initialize()


def viewer(xls):
    pd.set_option('display.max_rows', None)
    print(xls)
    menu(xls)


def headerRip(xls):
    xls.drop([0, 1], axis=0).head(5)
    print('Headers have been dropped')
    menu(xls)


def menu(xls):
    print("What would you like to do with this spread sheet?")
    print('press a button to do one of the following.')
    print('..........................................')
    print('1. upload items to TAMS Database.')
    print('2. view data in this excel sheet.')
    print('3. load different excel worksheet.')
    print('9. rip excel sheet headers.')
    print('Press any other key to quit.')
    print('..........................................')

    userSelection = input()
    if userSelection == '9':
        headerRip(xls)
    elif userSelection == '1':
        dbUploader(xls)
    elif userSelection == '2':
        viewer(xls)
    elif userSelection == '3':
        main()
    elif userSelection == '8':
        test = cursor.execute("SELECT id,serialNumber,skuId,locationId,statusId FROM itemSerialized WHERE id = ?",
                              (1)).fetchone()
        print(test)
        print(test[0])  # id
        print(test[1])  # serialnumber
        print(test[2])  # skuId
        print(test[3])  # locationId
        print(test[4])  # statusId
    else:
        print("Thanks for using Nik's data uploader.")
        locationFile.close()
        skuFile.close()
        itemSerializedFile.close()


def initialize():
    print("what is the name of the Excel file you would like to load?")
    print("Or press 1 to exit.")
    root = Tk()
    root.withdraw()
    excelFileName =  root.filename = filedialog.askopenfilename(initialdir="/", title="Select file",
                                               filetypes=(("excel files", "*.xls"), ("all files", "*.*")))
    if path.exists(excelFileName):
        xls = pd.read_excel(excelFileName)
        if xls.empty == True:
            print('DataFrame is empty.')
            main()
        else:
            print(xls.head())
            menu(xls)
    elif excelFileName == '1':
        print('Terminated.')
    else:
        print('path does not exist')
        main()


def main():
    print('Setting current working directory.')
    os.getcwd()
    print('.......................................')
    initialize()


if __name__ == "__main__":
    main()
