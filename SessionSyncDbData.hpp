/*******************************************************************************
 * Project:  NebulaMydis
 * @file     SessionSyncDbData.hpp
 * @brief    数据同步
 * @author   Bwar
 * @date:    2018年7月15日
 * @note     因连接问题或数据库故障写redis成功但写数据库失败的请求都会被写到文件，
 * 待故障恢复后再从文件中读出并重新写入数据库。
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_SESSIONSYNCDBDATA_HPP_
#define MYDIS_SESSIONSYNCDBDATA_HPP_

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <map>
#include "util/CBuffer.hpp"
#include "codec/CodecProto.hpp"
#include "actor/session/Session.hpp"
#include "pb/mydis.pb.h"
#include "StepSyncToDb.hpp"

namespace mydis
{

class StepSyncToDb;

class SessionSyncDbData: public neb::Session, public neb::DynamicCreator<SessionSyncDbData, std::string, std::string, ev_tstamp>
{
public:
    SessionSyncDbData(const std::string& strTableName, const std::string& strDataPath, ev_tstamp dSessionTimeout = 0.0);
    virtual ~SessionSyncDbData();

    virtual neb::E_CMD_STATUS Timeout();

    /**
     * @brief 是否需要延迟发送
     * @param oMemOperate 请求存储操作数据
     * @note 检查是否需要延迟发送，返回true表示需要，返回false表示不需要，如果被延迟发送，则当前发送操作
     * 默认为发送成功。数据将被存到本地文件，等下次数据请求成功再将数据发送出去。
     * @return 是否需要延迟
     */
    bool IsSendDelay(const neb::Mydis& oMemOperate) const;

    /**
     * @brief 同步数据到本地文件
     * @param oMemOperate 请求存储操作数据
     * @return 是否成功同步
     */
    bool SyncData(const neb::Mydis& oMemOperate);

    /**
     * @brief 添加需要同步的数据落点信息
     * @param oLocate 数据存储落点信息
     */
    void AddSyncLocate(const neb::Result::DataLocate& oLocate);
    void AddSyncLocate(uint32 uiSectionFrom, uint32 uiSectionTo);

    /**
     * @brief 获取同步文件中的数据
     * @param[out] oMsgHead 数据包头
     * @param[out] oMsgBody 数据包体
     * @return 是否获取到数据
     */
    bool GetSyncData(MsgHead& oMsgHead, MsgBody& oMsgBody);

    /**
     * @brief 数据发送失败或处理失败回退读指针
     * @param oMsgHead  发送失败的数据包头
     * @param oMsgBody  发送失败的数据包体
     */
    void GoBack(const MsgHead& oMsgHead, const MsgBody& oMsgBody);

    /**
     * @brief 扫描目录下需同步的文件
     * @return 是否扫描成功
     */
    bool ScanSyncData();

private:
    std::string m_strTableName;
    std::string m_strDataPath;
    neb::CBuffer* m_pBuff;
    std::unique_ptr<neb::CodecProto> m_pCodec;
    int m_iSendingSize;
    int m_iReadingFd;
    int m_iWritingFd;
    std::string m_strReadingFile;
    std::string m_strWritingFile;
    std::map<uint32, uint32> m_mapSyncLocate;
    std::set<std::string> m_setDataFiles;

public:
    std::shared_ptr<neb::Step> m_pStepSyncToDb;
};

} /* namespace mydis */

#endif /* MYDIS_SESSIONSYNCDBDATA_HPP_ */
