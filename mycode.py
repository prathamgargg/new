

import mysql.connector
import redshift_connector
import math

def Diff(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif


def inter(lst1, lst2):
    return [value for value in lst1 if value in lst2]

def getconnectionredshift(hostname , username , passwd , databasename) :
    temp = redshift_connector.connect(
        host = hostname ,
        user = username ,
        password = passwd ,
        database = databasename
    )
    return temp

def getrowcount(connection_name, database , schema , table) :
    curs = connection_name.cursor()
    curs.execute(f"select count(*) from {database}.{schema}.{table}")
    return curs.fetchall()[0][0]

def getredshiftcolname(connection_name, database , schema , table) :
    curs = connection_name.cursor()
    curs.execute(f""" select (col_name) from pg_get_cols('{database}.{schema}.{table}')
                   cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int) """)
    return curs.fetchall()

def getredshiftcolandtype(connection_name, database , schema , table) :
    curs = connection_name.cursor()
    curs.execute(f""" select (col_name) , (col_type) from pg_get_cols('{database}.{schema}.{table}')
                       cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int) """)
    return curs.fetchall()

def getmostfrequentstr(connection_name , colname , database , schema , table) :
    curs = connection_name.cursor()
    curs.execute(f"""SELECT {colname} from {database}.{schema}.{table} group by {colname} order by count({colname}) DESC limit 50""")
    return curs.fetchall()

def getavg(connection_name , colname , database , schema , table) :
    curs = connection_name.cursor()
    curs.execute(f"SELECT avg({colname}) from {database}.{schema}.{table}")
    return curs.fetchall()

def getmode(connection_name , colname , database , schema , table) :
    curs = connection_name.cursor()
    curs.execute(f"SELECT TOP 1 {colname} from {database}.{schema}.{table} group by {colname} order by count(*) desc")
    return curs.fetchall()


def main() :

    D1 = input()
    S1 = input()
    T1 = input()
    D2 = input()
    S2 = input()
    T2 = input()

    reddie = getconnectionredshift('payu-processing-cluster.cklrxyukhozv.ap-south-1.redshift.amazonaws.com',
                                   'pratham_grag' , 'Vdeu56fD' ,D1 )
    reddie1 = getconnectionredshift('payu-processing-cluster.cklrxyukhozv.ap-south-1.redshift.amazonaws.com',
                                    'pratham_grag' , 'Vdeu56fD' , D2 )

    print("difference in row count :",getrowcount(reddie,D1,S1,T1)-getrowcount(reddie1,D2,S2,T2))

    x = getredshiftcolname(reddie,D1,S1,T1)
    y = getredshiftcolname(reddie1,D2,S2,T2)
    print("column difference count :" ,len(x)-len(y))
    x = list(x)
    y = list(y)
    diff = Diff(x,y)
    print("column which is not common :" , diff)
    coln = inter(x,y)
    typ = {}

    var = getredshiftcolandtype(reddie,D1,S1,T1)

    for x , y in var :
        typ[x] = y

    coln = [x[0] for x in coln ]
    print(coln)

    s = "character"
    s1 = "double"
    s2 = "text"

    same_col = []
    dif_col_char = []
    dif_col_int = []

    for x in coln  :
        if s in typ[x] or s2 in typ[x]:
            a = getmostfrequentstr(reddie,x,D1,S1,T1)
            b = getmostfrequentstr(reddie1,x,D2,S2,T2)
            if a==b :
                same_col.append(x)
            else :
                dif_col_char.append(x)
        elif s1 in typ[x]:
            a = getavg(reddie,x,D1,S1,T1)
            b = getavg(reddie1,x,D2,S2,T2)

            a1 = getmode(reddie,x,D1,S1,T1)
            b1 = getmode(reddie1,x,D2,S2,T2)

            if a==b and a1==b1:
                same_col.append(x)
            else :
                cur = {
                    "mean_diff" : a[0][0]-b[0][0] ,
                     "mode_a" : a1[0][0] ,
                    "mode_b" : b1[0][0]
                }
                new = {}
                new[x] = cur
                dif_col_int.append(new)

    print("same col :" , same_col)
    print("different string type columns : " ,dif_col_char)
    print("different int type columns :",dif_col_int)

if __name__ == "__main__":
    main()


# reddie = redshift_connector.connect(
#     host = 'payu-processing-cluster.cklrxyukhozv.ap-south-1.redshift.amazonaws.com',
#     user = 'pratham_grag',
#     password = 'Vdeu56fD',
#     database = D1
# )
# reddie1 = redshift_connector.connect(
#     host = 'payu-processing-cluster.cklrxyukhozv.ap-south-1.redshift.amazonaws.com',
#     user = 'pratham_grag',
#     password = 'Vdeu56fD',
#     database = D2
# )

# consol : redshift_connector.Cursor = reddie.cursor()
# datapl : redshift_connector.Cursor = reddie1.cursor()

# consol.execute(f"select count(*) from {D1}.{S1}.{T1}")
# datapl.execute(f"select count(*) from {D2}.{S2}.{T2}")

# print("difference in row count :",consol.fetchall()[0][0]-datapl.fetchall()[0][0])


# consol.execute(f""" select (col_name) from pg_get_cols('{D1}.{S1}.{T1}')
#                   cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int) """)
# datapl.execute(f""" select (col_name) from pg_get_cols('{D2}.{S2}.{T2}')
#                   cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int) """)
# x = (consol.fetchall())
# y = (datapl.fetchall())
# print("column difference count :" ,len(x)-len(y))
# x = list(x)
# y = list(y)
#
# diff = Diff(x,y)
#
# print("column which is not common :" , diff)
#
# coln = inter(x,y)
#
# typ = {}
# consol.execute(f"""select (col_name) , (col_type) from pg_get_cols('{D1}.{S1}.{T1}')
#                 cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int)""")
# var = consol.fetchall()
#
#
# for x , y in var :
#     typ[x] = y
#
# coln = [x[0] for x in coln ]
# # print(coln)
# # print(typ)
#
# s = "character"
# s1 = "double"
# s2 = "text"
#
# same_col = []
# dif_col_char = []
# dif_col_int = []
#


# for x in coln  :
#     if s in typ[x] or s2 in typ[x]:
#         consol.execute(f"SELECT {x} from {D1}.{S1}.{T1} group by {x} order by count({x}) DESC limit 50")
#         datapl.execute(f"SELECT {x} from {D2}.{S2}.{T2} group by {x} order by count({x}) DESC limit 50")
#         a = consol.fetchall()
#         b = datapl.fetchall()
#         if a==b :
#             same_col.append(x)
#         else :
#             dif_col_char.append(x)
#     elif s1 in typ[x]:
#         consol.execute(f"SELECT avg({x}) from {D1}.{S1}.{T1}")
#         datapl.execute(f"SELECT avg({x}) from {D2}.{S2}.{T2}")
#         a = consol.fetchall()
#         b = datapl.fetchall()
#         consol.execute(f"SELECT TOP 1 {x} from {D1}.{S1}.{T1} group by {x} order by count(*) desc")
#         datapl.execute(f"SELECT TOP 1 {x} from {D2}.{S2}.{T2} group by {x} order by count(*) desc")
#         a1 = consol.fetchall()
#         b1 = datapl.fetchall()
#         check = False
#         if a==b and a1==b1:
#             check = True
#
#         if check :
#             same_col.append(x)
#         else :
#             cur = {
#                 "mean_diff" : a[0][0]-b[0][0] ,
#                  "mode_a" : a1[0][0] ,
#                 "mode_b" : b1[0][0]
#             }
#             new = {}
#             new[x] = cur
#             dif_col_int.append(new)
#
# print("same col :" , same_col)
# print("different string type columns : " ,dif_col_char)
# print("different int type columns :",dif_col_int)