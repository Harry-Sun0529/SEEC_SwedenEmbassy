# -*- coding: utf-8 -*-

import json
import copyheaders
import pandas
import pymongo
import requests
import time
import random
import math
from datetime import datetime
from colorama import Fore, Style
import os
from lxml import etree


# 定义时间转换格式
def convertTime(timeStr):
    originalTime = datetime.strptime(timeStr, "%a %b %d %H:%M:%S %z %Y")

    # 格式化为正常时间字符串
    normalTime_str = originalTime.strftime("%Y-%m-%d %H:%M:%S")

    return normalTime_str


# 微博链接
# ===============================


headers = copyheaders.headers_raw_to_dict(b"""
Accept:application/json, text/plain, */*
Client-Version:v2.45.41
Cookie:SCF=Au11V-R8mBJfrVL-ElAuyRjhTOSM6PNwutXqpN2KwMUWuI9ZSQJG7aL3Q8b_OWCTKnfRWB2wIltXQC37u1erTls.; SINAGLOBAL=1553660159760.0042.1739524809891; UOR=,,tophub.today; ALF=1746852555; SUB=_2A25K8z2bDeRhGeBJ4lUY8y7EyjWIHXVmcT9TrDV8PUJbkNANLWnTkW1NRjn6U1JtgZEqAjzBBjUONw8qrLWmDJjp; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5n9zJq2EFVW1sfyACKT7rj5JpX5KMhUgL.FoqN1KM4e05ReK.2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMcS0.N1Ke71h24; WBPSESS=S8MbxEPPgMhJABbt7NPbA2Npqq-HVmdZxa3BBs6L-oe6tsqMLeicPhMYoMkQkp3JYVAG2OkdVLh5cgKRqL_sK0rF_W29k7QGNc5MY53dSjRWSr_cfB_CfEx_9oHPH0VP609iDnKrsgpvksfVIyKC-A==; XSRF-TOKEN=cXIGDk-fXgL1DmT-dFkLNmyV; _s_tentry=weibo.com; Apache=9614959947481.559.1744684468324; ULV=1744684468325:77:16:2:9614959947481.559.1744684468324:1744603646819
Priority:u=1, i
Referer:https://weibo.com/u/7908422218
Sec-Ch-Ua:"Microsoft Edge";v="125", "Chromium";v="125", "Not.A/Brand";v="24"
Sec-Ch-Ua-Mobile:?0
Sec-Ch-Ua-Platform:"Windows"
Sec-Fetch-Dest:empty
Sec-Fetch-Mode:cors
Sec-Fetch-Site:same-origin
Server-Version:v2024.06.11.3
User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0
X-Requested-With:XMLHttpRequest
X-Xsrf-Token:ddtQYfGspdWWiqY-TChDO-iu
""")

weibo_base_headers = {
    "X-Xsrf-Token": '',
    "X-Requested-With": 'XMLHttpRequest',
    "User-Agent": 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36 Edg/115.0.1901.203',
    "Referer": 'https://m.weibo.cn/search?containerid=231522type%3D1%26t%3D10%26q%3D%231%23&isnewpage=1&luicode=10000011&lfid=100103type%3D1%26q%3D1',
    "Accept": 'application/json, text/plain, */*',
    "Cookie": 'SCF=Au11V-R8mBJfrVL-ElAuyRjhTOSM6PNwutXqpN2KwMUWuI9ZSQJG7aL3Q8b_OWCTKnfRWB2wIltXQC37u1erTls.; SINAGLOBAL=1553660159760.0042.1739524809891; UOR=,,tophub.today; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5n9zJq2EFVW1sfyACKT7rj5JpX5KMhUgL.FoqN1KM4e05ReK.2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMcS0.N1Ke71h24; ALF=1746247085; SUB=_2A25K6mD9DeRhGeBJ4lUY8y7EyjWIHXVphvw1rDV8PUJbkNANLRfkkW1NRjn6U1XdT8S2Xg_3WZMVZn8y1A28sCW2; WBPSESS=S8MbxEPPgMhJABbt7NPbA2Npqq-HVmdZxa3BBs6L-oe6tsqMLeicPhMYoMkQkp3JYVAG2OkdVLh5cgKRqL_sK5RO7bqFTAEHUr4yvfsQHVNW_JSZi_iVMQLbuHABnHqTZDOq-OqrUNWT5QscXRVgpQ==; ULV=1744078989650:67:6:2:9704383013777.844.1744078989618:1743993174291; XSRF-TOKEN=w9bekWz3nU1Tr_wSZIG3BHfT'
}


def send_get(api, headers, params):
    print(f'>>>访问：{api}')
    while 1:
        try:
            res = requests.get(
                api,
                headers=headers,
                timeout=(4, 5),
                params=params,

            )

            if '访问频次过高' in res.text:
                print('访问频次过高')
                time.sleep(10)
                continue

            if '<title>ÐÂÀËÍ¨ÐÐÖ¤</title>' in res.text:
                print(f'获取用户失败，继续：')
                time.sleep(1)

                continue

            if res.status_code != 200:
                print(res.text)
                time.sleep(10)
                continue
            time.sleep(.8)
            return res.json()
        except Exception as e:
            print(f'some error:{e}')
            time.sleep(1)


def get_html_pure_text(text):
    try:
        return etree.HTML(text).xpath("string(.)")
    except:
        return text


def getlongtext(mid):
    api = f'https://m.weibo.cn/statuses/extend?id={mid}'
    response = send_get(api, weibo_base_headers, {})
    return get_html_pure_text(response.get("data").get("longTextContent"))


def download_user_info(usercode, username):
    # 定义空数据表

    print('https://weibo.com/ajax/profile/info?custom={}'.format(usercode))
    response = send_get('https://weibo.com/ajax/profile/info?custom={}'.format(usercode), headers=headers, params={})
    print(response)
    data = response['data']['user']
    name = data['screen_name']
    ID = data['id']
    conNum = data['statuses_count']

    if os.path.exists('./datas/{}.xlsx'.format(name)):
        return print(f"曾采集：{name}")

    pageNum = math.ceil(conNum / 20)

    pageDict = dict(zip(range(1, pageNum + 1), [False for _ in range(1, pageNum + 1)]))
    index = 0
    since_id = None
    totalStart = datetime.now()
    retryTime = 0
    for I in range(1, pageNum + 1):
        while pageDict[I] == False:
            # try:
            startTime = datetime.now()

            if I == 1:
                param = {'uid': str(ID),
                         'page': I,
                         'feature': 0,
                         }
            else:
                param = {'uid': str(ID),
                         'page': I,
                         'feature': 0,
                         'since_id': since_id
                         }

            url = 'https://weibo.com/ajax/statuses/mymblog'

            response = send_get(url, headers, param)
            dataJson = response
            if dataJson.get("data") is None:
                print(dataJson)
                return
            data = dataJson['data']
            since_id = data['since_id']
            dataList = data['list']
            print(f'pagelength:{len(dataList)}')
            if len(dataList) == 0:
                print(f'null:{pageNum}')
                time.sleep(1)
                return



            for J in dataList:
                text = J['text_raw']

                re = J['reposts_count']
                co = J['comments_count']
                at = J['attitudes_count']
                ti = convertTime(J['created_at'])

                try:

                    saveitem = {}
                    saveitem["_id"] = J.get("mid", '')
                    saveitem["博主id"] = usercode
                    saveitem["博主名称"] = username
                    saveitem["帖子id"] = J.get("mid", '')
                    saveitem["发表时间"] = ti

                    if J.get("isLongText"):
                        saveitem["发布内容"] = getlongtext(J.get("mid", ''))
                    else:
                        saveitem["发布内容"] = get_html_pure_text(J.get("text_raw", ''))

                    saveitem["帖子视频"] = ""
                    if J.get("page_info", {}).get("object_type") == 'video':
                        saveitem["帖子视频"] = J.get("page_info", {}).get("media_info", {}).get("mp4_hd_url")
                    saveitem["视频id"] = saveitem["帖子视频"].split(".mp4")[0].split("/")[-1]
                    saveitem["帖子图片"] = ";".join(
                        [f'https://wx1.sinaimg.cn/orj360/{i}.jpg' for i in J.get("pic_ids", [])])
                    saveitem["图片id"] = "; ".join(J.get("pic_ids", []))

                    saveitem["点赞数"] = at
                    saveitem["转发数"] = re
                    saveitem["评论数"] = co

                    saveitem["博文地址"] = f'https://weibo.com/{usercode}/{J["mblogid"]}'
                    saveitem['文本长度'] = J.get("textLength")
                    saveitem['发布地点'] = J.get("region_name")

                    saveitem["相关话题"] = "; ".join([i.get("topic_title") for i in J.get("topic_struct", [])])

                    if J.get("retweeted_status") is not None:
                        saveitem["是否原创"] = '转发'

                        if J.get("retweeted_status", {}).get("isLongText") == True:
                            saveitem["源微博正文"] = get_html_pure_text(
                                getlongtext(J.get("retweeted_status", {}).get("mid")))
                        else:
                            saveitem["源微博正文"] = get_html_pure_text(J.get("retweeted_status", {}).get("text"))

                    else:
                        saveitem["是否原创"] = '原创'
                        saveitem["源微博正文"] = ""


                    try:
                            all_rs_contains.append(saveitem)
                    except Exception as e:
                            print(f'some error:{e}')


                except Exception as e:
                    print(f'handle_error:{e}')
                    continue

                index += 1
                print(Fore.RED + Style.BRIGHT + "博文" + Style.RESET_ALL, end='：')
                print(text)

                print(Fore.RED + Style.BRIGHT + "时间" + Style.RESET_ALL, end='：')
                print(ti)

                print(Fore.RED + Style.BRIGHT + "点赞" + Style.RESET_ALL, end='：')
                print(at)

                print(Fore.RED + Style.BRIGHT + "评论" + Style.RESET_ALL, end='：')
                print(co)

                print(Fore.RED + Style.BRIGHT + "转发" + Style.RESET_ALL, end='：')
                print(re)

                print('-' * 20)
                sleepTime = random.random()
                endTime = datetime.now()
                useTime = (endTime - startTime).seconds + 2 + sleepTime

                rest = I / pageNum * 100
                restTime = useTime * (pageNum - I)
                totalUsedTime = (endTime - totalStart).seconds
                print(username+'==第{}/{}页==进度{:.5f}%==预计剩余{:.2f}小时==已耗时{:.2f}小时==重试次数{}=='.format(I, pageNum,
                                                                                                            rest,
                                                                                                            restTime / 60 / 60,
                                                                                                            totalUsedTime / 60 / 60,
                                                                                                            retryTime))

                pageDict[I] = True


if __name__ == "__main__":


        all_rs_contains = []

        download_user_info(usercode='3260734291', username='瑞典大使馆')

        df = pandas.DataFrame(all_rs_contains)
        writer = pandas.ExcelWriter(f'博文表.xlsx', engine='xlsxwriter', options={'strings_to_urls': False})
        df.to_excel(writer, index=False)
        writer.save()
