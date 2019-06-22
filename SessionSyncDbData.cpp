/*******************************************************************************
 * Project:  NebulaMydis
 * @file     SessionSyncDbData.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年7月15日
 * @note
 * Modify history:
 ******************************************************************************/
#include "SessionSyncDbData.hpp"

namespace mydis
{

SessionSyncDbData::SessionSyncDbData(const std::string& strTableName, const std::string& strDataPath, ev_tstamp dSessionTimeout)
    : neb::Session(strTableName, dSessionTimeout),
      m_strTableName(strTableName), m_strDataPath(strDataPath),
      m_pBuff(NULL), m_pCodec(nullptr), m_iSendingSize(0), m_iReadingFd(-1), m_iWritingFd(-1),
      m_pStepSyncToDb(nullptr)
{
    m_pBuff = new neb::CBuffer();
}

SessionSyncDbData::~SessionSyncDbData()
{
    if (m_pBuff != NULL)
    {
        delete m_pBuff;
        m_pBuff = NULL;
    }
    if (-1 != m_iReadingFd)
    {
        close(m_iReadingFd);
    }
    m_mapSyncLocate.clear();
    m_setDataFiles.clear();
}

neb::E_CMD_STATUS SessionSyncDbData::Timeout()
{
    if (0 == m_setDataFiles.size())
    {
        return(neb::CMD_STATUS_COMPLETED);
    }
    MsgHead oMsgHead;
    MsgBody oMsgBody;
    if (GetSyncData(oMsgHead, oMsgBody))
    {
        m_pStepSyncToDb = MakeSharedStep("mydis::StepSyncToDb", m_strTableName, oMsgHead.cmd(), oMsgHead.seq(), oMsgBody);
        if (nullptr != m_pStepSyncToDb)
        {
            if (neb::CMD_STATUS_RUNNING == m_pStepSyncToDb->Emit(neb::ERR_OK))
            {
                return(neb::CMD_STATUS_RUNNING);
            }
        }
    }
    return(neb::CMD_STATUS_COMPLETED);
}

bool SessionSyncDbData::IsSendDelay(const neb::Mydis& oMemOperate) const
{
    std::map<uint32, uint32>::const_iterator c_iter = m_mapSyncLocate.lower_bound(oMemOperate.section_factor());
    if (c_iter == m_mapSyncLocate.end())
    {
        return(false);
    }
    else
    {
        if (oMemOperate.section_factor() < c_iter->second)
        {
            return(false);
        }
        else
        {
            return(true);
        }
    }
}

bool SessionSyncDbData::SyncData(const neb::Mydis& oMemOperate)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (nullptr == m_pCodec)
    {
#if __cplusplus >= 201401L
        m_pCodec = std::make_unique<neb::CodecProto>(GetLogger(), neb::CODEC_PROTOBUF);
#else
        m_pCodec = std::unique_ptr<neb::CodecProto>(new neb::CodecProto(GetLogger(), neb::CODEC_PROTOBUF));
#endif
    }
    char szWriteFileName[64] = {0};
    std::map<uint32, uint32>::const_iterator c_iter = m_mapSyncLocate.lower_bound(oMemOperate.section_factor());
    if (c_iter == m_mapSyncLocate.end())
    {
        snprintf(szWriteFileName, sizeof(szWriteFileName), "%u.%s.%s.dat", GetWorkerIndex(),
                        oMemOperate.db_operate().table_name().c_str(),
                        ltime_t2TimeStr(GetNowTime(), "YYYYMMDDHHMI").c_str());
    }
    else
    {
        if (oMemOperate.section_factor() < c_iter->second)
        {
            snprintf(szWriteFileName, sizeof(szWriteFileName), "%u.%s.%s.dat", GetWorkerIndex(),
                            oMemOperate.db_operate().table_name().c_str(),
                            ltime_t2TimeStr(GetNowTime(), "YYYYMMDDHHMI").c_str());
        }
        else
        {
            snprintf(szWriteFileName, sizeof(szWriteFileName), "%u.%s.%s.%u.%u.dat", GetWorkerIndex(),
                            oMemOperate.db_operate().table_name().c_str(),
                            ltime_t2TimeStr(GetNowTime(), "YYYYMMDDHHMI").c_str(), c_iter->second, c_iter->first);
        }
    }
    if (m_strWritingFile != std::string(szWriteFileName) || m_iWritingFd < 0)
    {
        if (m_iWritingFd > 0)
        {
            close(m_iWritingFd);
            m_iWritingFd = -1;
            m_strWritingFile.clear();
        }
        m_iWritingFd = open(std::string(m_strDataPath + szWriteFileName).c_str(), O_CREAT|O_APPEND|O_WRONLY, S_IRUSR|S_IWUSR);
        if (m_iWritingFd < 0)
        {
            LOG4_ERROR("open file %s error %d", std::string(m_strDataPath + szWriteFileName).c_str(), errno);
            return(false);
        }
        m_strWritingFile = szWriteFileName;
        m_setDataFiles.insert(m_strWritingFile);
    }

    MsgHead oMsgHead;
    MsgBody oMsgBody;
    oMsgBody.set_data(oMemOperate.SerializeAsString());
    oMsgHead.set_cmd(CMD_REQ_STORATE);
    oMsgHead.set_seq(0);
    oMsgHead.set_msgbody_len(oMsgBody.ByteSize());

    int iNeedWriteLen = 0;
    int iWriteLen = 0;
    iNeedWriteLen = oMsgHead.ByteSize();
    while (iWriteLen < iNeedWriteLen)
    {
        iWriteLen += write(m_iWritingFd, oMsgHead.SerializeAsString().c_str() + iWriteLen, iNeedWriteLen - iWriteLen);
    }
    iNeedWriteLen = oMsgHead.msgbody_len();
    while (iWriteLen < iNeedWriteLen)
    {
        iWriteLen += write(m_iWritingFd, oMsgBody.SerializeAsString().c_str() + iWriteLen, iNeedWriteLen - iWriteLen);
    }
    return(true);
}

void SessionSyncDbData::AddSyncLocate(const neb::Result::DataLocate& oLocate)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    std::map<uint32, uint32>::iterator iter = m_mapSyncLocate.lower_bound(oLocate.section_to());
    if (iter == m_mapSyncLocate.end())
    {
        m_mapSyncLocate.insert(std::make_pair(oLocate.section_to(), oLocate.section_from()));
    }
}

void SessionSyncDbData::AddSyncLocate(uint32 uiSectionFrom, uint32 uiSectionTo)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    std::map<uint32, uint32>::iterator iter = m_mapSyncLocate.lower_bound(uiSectionTo);
    if (iter == m_mapSyncLocate.end())
    {
        m_mapSyncLocate.insert(std::make_pair(uiSectionTo, uiSectionFrom));
    }
}

bool SessionSyncDbData::GetSyncData(MsgHead& oMsgHead, MsgBody& oMsgBody)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (nullptr == m_pCodec)
    {
#if __cplusplus >= 201401L
        m_pCodec = std::make_unique<neb::CodecProto>(GetLogger(), CODEC_PROTOBUF);
#else
        m_pCodec = std::unique_ptr<neb::CodecProto>(new neb::CodecProto(GetLogger(), CODEC_PROTOBUF));
#endif
    }
    if (m_iReadingFd < 0)
    {
        if (0 == m_setDataFiles.size())
        {
            return(false);
        }
        m_strReadingFile = *(m_setDataFiles.begin());
        if (m_strReadingFile == m_strWritingFile)
        {
            return(false);
        }
        m_iReadingFd = open(std::string(m_strDataPath + m_strReadingFile).c_str(), O_RDONLY);
        if (m_iReadingFd < 0)
        {
            LOG4_ERROR("open file %s error %d", std::string(m_strDataPath + m_strReadingFile).c_str(), errno);
            return(false);
        }
    }

    E_CODEC_STATUS eCodecStatus = m_pCodec->Decode(m_pBuff, oMsgHead, oMsgBody);
    switch (eCodecStatus)
    {
        case CODEC_STATUS_OK:
            m_iSendingSize = oMsgHead.ByteSize() + oMsgHead.msgbody_len();
            return(true);
        case CODEC_STATUS_ERR:
            return(false);
        case CODEC_STATUS_PAUSE:
            break;
        default:
            return(false);
    }
    int iErrno = 0;
    int iReadLen = 0;
    do
    {
        iReadLen = m_pBuff->ReadFD(m_iReadingFd, iErrno);
        eCodecStatus = m_pCodec->Decode(m_pBuff, oMsgHead, oMsgBody);
        if (CODEC_STATUS_OK == eCodecStatus)
        {
            m_iSendingSize = oMsgHead.ByteSize() + oMsgHead.msgbody_len();
            return(true);
        }
    }
    while (iReadLen > 0);

    close(m_iReadingFd);
    m_iReadingFd = -1;
    unlink(std::string(m_strDataPath + m_strReadingFile).c_str());
    m_setDataFiles.erase(m_strReadingFile);
    if (0 == m_strReadingFile.size())
    {
        m_mapSyncLocate.clear();
    }
    m_strReadingFile.clear();

    return(GetSyncData(oMsgHead, oMsgBody));
}

void SessionSyncDbData::GoBack(const MsgHead& oMsgHead, const MsgBody& oMsgBody)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (nullptr == m_pCodec)
    {
#if __cplusplus >= 201401L
        m_pCodec = std::make_unique<neb::CodecProto>(GetLogger(), CODEC_PROTOBUF);
#else
        m_pCodec = std::unique_ptr<neb::CodecProto>(new neb::CodecProto(GetLogger(), CODEC_PROTOBUF));
#endif
    }
    if (m_iSendingSize == (oMsgHead.ByteSize() + (int)oMsgHead.msgbody_len()))
    {
        if ((int)(m_pBuff->GetReadIndex()) >= m_iSendingSize)
        {
            m_pBuff->SetReadIndex((int)(m_pBuff->GetReadIndex()) - m_iSendingSize);
        }
        else
        {
            lCBuffer* pBuff = new lCBuffer();
            m_pCodec->Encode(oMsgHead, oMsgBody, pBuff);
            pBuff->Write(m_pBuff, m_pBuff->ReadableBytes());
            delete m_pBuff;
            m_pBuff = pBuff;
        }
    }
}

bool SessionSyncDbData::ScanSyncData()
{
    LOG4_TRACE("%s()", __FUNCTION__);
    DIR* dir;
    struct dirent* dirent_ptr;
    dir = opendir(m_strDataPath.c_str());
    if (dir != NULL)
    {
        size_t uiCurrentPos = 0;
        size_t uiNextPos = 0;
        std::string strFileName;
        std::string strWorkerIndex;
        while ((dirent_ptr = readdir(dir)) != NULL)
        {
            strFileName = dirent_ptr->d_name;
            uiNextPos = strFileName.find('.', uiCurrentPos);
            strWorkerIndex = strFileName.substr(uiCurrentPos, uiNextPos - uiCurrentPos);
            if (std::string::npos != strFileName.find(m_strTableName)
                            && strtoul(strWorkerIndex.c_str(), NULL, 10) == GetWorkerIndex())
            {
                m_setDataFiles.insert(strFileName);
            }
        }
        closedir(dir);
        return(true);
    }
    else
    {
        LOG4_ERROR("open dir %s error %d", m_strDataPath.c_str(), errno);
        return(false);
    }
}

} /* namespace mydis */
