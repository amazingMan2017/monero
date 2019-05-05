import urllib
import httplib
import json
import sys
import xlrd
import xlwt
import datetime

def write_result_to_excel(sheet, start_row, response_str):
    try:
        #print 'write result to excel start row is ' + str(start_row)
        final_row = start_row
        result_json = json.loads(response_str)
        diffs_array = result_json['result']['diffs']
        for d in diffs_array:
            print(d['blockheight'],d['totalwork'],d['timespan'],d['difficulty'],d['logtime'])
            sheet.write(final_row, 0, d['blockheight'])
            sheet.write(final_row, 1, d['totalwork'])
            sheet.write(final_row, 2, d['timespan'])
            sheet.write(final_row, 3, d['difficulty'])
            sheet.write(final_row, 4, d['logtime'])
            final_row += 1
    except Exception, e:
        print e
    finally:
        #print("return final row is " + str(final_row))
        return final_row


def get_difficulty_statists(sheet, start_row, height):
    #print height
    try:
        params_json = {"jsonrpc": "2.0", "id": "0", "method": "get_difficulty_statistics_by_height",
                       "params": {"height": 1}}
        # set request height to argv[1]
        params_json['params']['height'] = height
        params = json.dumps(params_json)
        headers = {'Content-type': 'application/json'}
        httpClient = httplib.HTTPConnection('127.0.0.1', 28081, timeout=10)
        httpClient.request('POST', '/json_rpc', params, headers)
        response = httpClient.getresponse()
        # print response.status
        # print response.reason
        # print response.read()
        # load json and write to excel
        return write_result_to_excel(sheet, start_row, response.read())

    except Exception, e:
        print e
    finally:
        if httpClient:
            httpClient.close()


def main():
    if len(sys.argv) <= 1:
        print 'error arguments must big than 1'
        return
    else:
        print 'get difficulty statistics for block height ' + sys.argv[1]

    # open the excel file
    writebook = xlwt.Workbook()
    # get a sheet named
    sheet = writebook.add_sheet('difficulty_statistics')

    start_row = 0
    blockheight = int(sys.argv[1])
    for height in range(1, blockheight):
        start_row = get_difficulty_statists(sheet, start_row, height)


    # save the excel
    writebook.save('difficulty_statistics.xls')


if __name__ == '__main__':
    main()
