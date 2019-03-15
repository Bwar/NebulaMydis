/*******************************************************************************
 * Project:  NebulaMydis
 * @file     CmdMydis.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#include <fstream>
#include "CmdMydis.hpp"

namespace mydis
{

CmdMydis::CmdMydis(int32 iCmd)
    : neb::Cmd(iCmd),
      m_pErrBuff(NULL), m_pRedisNodeSession(nullptr),
      m_pStepSendToDbAgent(nullptr), m_pStepReadFromRedis(nullptr),
      m_pStepWriteToRedis(nullptr), m_pStepReadFromRedisForWrite(nullptr),
      m_pStepSyncToDb(nullptr)
{
    m_pErrBuff = (char*)malloc(gc_iErrBuffSize);
}

CmdMydis::~CmdMydis()
{
    if (m_pErrBuff != NULL)
    {
        free(m_pErrBuff);
        m_pErrBuff = NULL;
    }
}

bool CmdMydis::Init()
{
    if (ReadDataProxyConf() && ReadTableRelation())
    {
        ScanSyncData();
        return(true);
    }
    return(false);
}

bool CmdMydis::ReadDataProxyConf()
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    neb::CJsonObject oConfJson;
    //配置文件路径查找
    std::string strConfFile = GetWorkPath() + std::string("/conf/mydis/CmdMydis.json");
    LOG4_TRACE("CONF FILE = %s.", strConfFile.c_str());

    std::ifstream fin(strConfFile.c_str());

    if (fin.good())
    {
        std::stringstream ssContent;
        ssContent << fin.rdbuf();
        fin.close();
        if (oConfJson.Parse(ssContent.str()))
        {
            LOG4_TRACE("oConfJson pasre OK");
            char szSessionId[64] = {0};
            char szFactorSectionKey[32] = {0};
            int32 iVirtualNodeNum = 200;
            int32 iHashAlgorithm = 0;
            uint32 uiDataType = 0;
            uint32 uiFactor = 0;
            uint32 uiFactorSection = 0;
            std::string strRedisNode;
            std::string strMaster;
            std::string strSlave;
            oConfJson.Get("hash_algorithm", iHashAlgorithm);
            oConfJson.Get("virtual_node_num", iVirtualNodeNum);
            if (oConfJson["cluster"].IsEmpty())
            {
                LOG4_ERROR("oConfJson[\"cluster\"] is empty!");
                return(false);
            }
            if (oConfJson["redis_group"].IsEmpty())
            {
                LOG4_ERROR("oConfJson[\"redis_group\"] is empty!");
                return(false);
            }
            for (int i = 0; i < oConfJson["data_type"].GetArraySize(); ++i)
            {
                if (oConfJson["data_type_enum"].Get(oConfJson["data_type"](i), uiDataType))
                {
                    for (int j = 0; j < oConfJson["section_factor"].GetArraySize(); ++j)
                    {
                        if (oConfJson["section_factor_enum"].Get(oConfJson["section_factor"](j), uiFactor))
                        {
                            if (oConfJson["factor_section"][oConfJson["section_factor"](j)].IsArray())
                            {
                                std::set<uint32> setFactorSection;
                                for (int k = 0; k < oConfJson["factor_section"][oConfJson["section_factor"](j)].GetArraySize(); ++k)
                                {
                                    if (oConfJson["factor_section"][oConfJson["section_factor"](j)].Get(k, uiFactorSection))
                                    {
                                        snprintf(szSessionId, sizeof(szSessionId), "SessionRedisNode-%u:%u:%u", uiDataType, uiFactor, uiFactorSection);
                                        snprintf(szFactorSectionKey, sizeof(szFactorSectionKey), "LE_%u", uiFactorSection);
                                        setFactorSection.insert(uiFactorSection);
                                        if (oConfJson["cluster"][oConfJson["data_type"](i)][oConfJson["section_factor"](j)][szFactorSectionKey].IsArray())
                                        {
                                            for (int l = 0; l < oConfJson["cluster"][oConfJson["data_type"](i)][oConfJson["section_factor"](j)][szFactorSectionKey].GetArraySize(); ++l)
                                            {
                                                std::shared_ptr<SessionRedisNode> pNodeSession = std::dynamic_pointer_cast<SessionRedisNode>(GetSession(szSessionId));
                                                if (pNodeSession == nullptr)
                                                {
                                                    pNodeSession = std::dynamic_pointer_cast<SessionRedisNode>(
                                                            MakeSharedSession("mydis::SessionRedisNode", std::string(szSessionId), iHashAlgorithm, iVirtualNodeNum, 0.0));
                                                }
                                                strRedisNode = oConfJson["cluster"][oConfJson["data_type"](i)][oConfJson["section_factor"](j)][szFactorSectionKey](l);
                                                if (oConfJson["redis_group"][strRedisNode].Get("master", strMaster)
                                                                && oConfJson["redis_group"][strRedisNode].Get("slave", strSlave))
                                                {
                                                    LOG4_TRACE("Add node %s[%s, %s] to %s!",
                                                                    strRedisNode.c_str(), strMaster.c_str(), strSlave.c_str(), szSessionId);
                                                    pNodeSession->AddRedisNode(strRedisNode, strMaster, strSlave);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            LOG4_ERROR("oConfJson[\"cluster\"][\"%s\"][\"%s\"][\"%s\"] is not a json array!",
                                                            oConfJson["data_type"](i).c_str(), oConfJson["section_factor"](j).c_str(), szFactorSectionKey);
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        LOG4_ERROR("oConfJson[\"factor_section\"][\"%s\"](%d) is not exist!",
                                                        oConfJson["section_factor"](j).c_str(), k);
                                        continue;
                                    }
                                }
                                snprintf(szSessionId, sizeof(szSessionId), "%u:%u", uiDataType, uiFactor);
                                LOG4_TRACE("add data_type and factor [%s] to m_mapFactorSection", szSessionId);
                                m_mapFactorSection.insert(std::pair<std::string, std::set<uint32> >(szSessionId, setFactorSection));
                            }
                            else
                            {
                                LOG4_ERROR("oConfJson[\"factor_section\"][\"%s\"] is not a json array!",
                                                oConfJson["section_factor"](j).c_str());
                                continue;
                            }
                        }
                        else
                        {
                            LOG4_ERROR("missing %s in oConfJson[\"section_factor_enum\"]", oConfJson["section_factor"](j).c_str());
                            continue;
                        }
                    }
                }
                else
                {
                    LOG4_ERROR("missing %s in oConfJson[\"data_type_enum\"]", oConfJson["data_type"](i).c_str());
                    continue;
                }
            }
        }
        else
        {
            LOG4_ERROR("oConfJson pasre error");
            return false;
        }
    }
    else
    {
        //配置信息流读取失败
        LOG4_ERROR("Open conf (%s) error!",strConfFile.c_str());
        return false;
    }

    return true;
}

bool CmdMydis::ReadTableRelation()
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    //配置文件路径查找
    std::string strConfFile = GetWorkPath() + std::string("/conf/mydis/CmdMydisTableRelative.json");
    LOG4_DEBUG("CONF FILE = %s.", strConfFile.c_str());

    std::ifstream fin(strConfFile.c_str());

    if (fin.good())
    {
        std::stringstream ssContent;
        ssContent << fin.rdbuf();
        fin.close();
        if (m_oJsonTableRelative.Parse(ssContent.str()))
        {
            return(true);
        }
        else
        {
            LOG4_ERROR("m_oJsonTableRelative parse error!");
        }
    }
    LOG4_ERROR("failed to open %s!", strConfFile.c_str());
    return(false);
}

bool CmdMydis::ScanSyncData()
{
    /*
    DIR* dir;
    struct dirent* dirent_ptr;
    dir = opendir(std::string(GetWorkPath() + "/conf/mydis/").c_str());
    if (dir != NULL)
    {
        size_t uiCurrentPos = 0;
        size_t uiNextPos = 0;
        std::string strFileName;
        std::string strWorkerIndex;
        std::string strTableName;
        std::string strTime;
        std::string strSectionFrom;
        std::string strSectionTo;
        std::map<uint32, uint32> mapSection;
        std::map<uint32, uint32>::iterator sec_iter;
        std::map<std::string, std::map<uint32, uint32> > mapTables;
        std::map<std::string, std::map<uint32, uint32> >::iterator tb_iter;
        mapSection.insert(std::pair<uint32, uint32>(0, 0));
        while ((dirent_ptr = readdir(dir)) != NULL)
        {
            strFileName = dirent_ptr->d_name;
            if (strFileName.size() > 4 && std::string(".dat") == strFileName.substr(strFileName.size() - 4, 4))
            {
                uiCurrentPos = 0;
                uiNextPos = 0;

                uiNextPos = strFileName.find('.', uiCurrentPos);
                strWorkerIndex = strFileName.substr(uiCurrentPos, uiNextPos - uiCurrentPos);
                uiCurrentPos = uiNextPos + 1;

                if (strtoul(strWorkerIndex.c_str(), NULL, 10) != GetWorkerIndex())
                {
                    continue;
                }

                uiNextPos = strFileName.find('.', uiCurrentPos);
                strTableName = strFileName.substr(uiCurrentPos, uiNextPos - uiCurrentPos);
                uiCurrentPos = uiNextPos + 1;

                uiNextPos = strFileName.find('.', uiCurrentPos);
                strTime = strFileName.substr(uiCurrentPos, uiNextPos - uiCurrentPos);
                uiCurrentPos = uiNextPos + 1;

                tb_iter = mapTables.find(strTableName);
                if (tb_iter == mapTables.end())
                {
                    mapTables.insert(std::make_pair(strTableName, mapSection));
                    tb_iter = mapTables.begin();
                }

                uiNextPos = strFileName.find('.', uiCurrentPos);
                if (std::string::npos == uiNextPos)
                {
                    continue;
                }
                strSectionFrom = strFileName.substr(uiCurrentPos, uiNextPos - uiCurrentPos);
                uiCurrentPos = uiNextPos + 1;

                uiNextPos = strFileName.find('.', uiCurrentPos);
                if (std::string::npos == uiNextPos)
                {
                    continue;
                }
                strSectionTo = strFileName.substr(uiCurrentPos, uiNextPos - uiCurrentPos);
                uiCurrentPos = uiNextPos + 1;

                tb_iter->second.insert(std::pair<uint32, uint32>(
                                strtoul(strSectionTo.c_str(), NULL, 10), strtoul(strSectionFrom.c_str(), NULL, 10)));
            }
        }
        closedir(dir);

        MsgHead oMsgHead;
        MsgBody oMsgBody;
        for (tb_iter = mapTables.begin(); tb_iter != mapTables.end(); ++tb_iter)
        {
            std::shared_ptr<SessionSyncDbData> pSessionSyncDbData
                = std::dynamic_pointer_cast<SessionSyncDbData>(MakeSharedSession(
                            "mydis::SessionSyncDbData", tb_iter->first,
                            std::string(GetWorkPath() + "/conf/mydis/"), 60.0));
            if (pSessionSyncDbData == nullptr)
            {
                LOG4_ERROR("failed to MakeSharedSession(SessionSyncDbData)");
                break;
            }
            for (sec_iter = tb_iter->second.begin(); sec_iter != tb_iter->second.end(); ++sec_iter)
            {
                if (0 == sec_iter->first && 0 == sec_iter->second)
                {
                    continue;
                }
                pSessionSyncDbData->AddSyncLocate(sec_iter->second, sec_iter->first);
            }

            pSessionSyncDbData->ScanSyncData();
            if (pSessionSyncDbData->GetSyncData(oMsgHead, oMsgBody))
            {
                m_pStepSyncToDb = MakeSharedStep(
                                "mydis::StepSyncToDb", tb_iter->first, oMsgHead, oMsgBody);
                if (m_pStepSyncToDb != nullptr)
                {
                    m_pStepSyncToDb->Emit(neb::ERR_OK);
                }
            }
        }

        return(true);
    }
    else
    {
        LOG4_ERROR("open dir %s error %d", std::string(GetWorkPath() + "/conf/Proxy/").c_str(), errno);
        return(false);
    }
    */
    return(true);
}

bool CmdMydis::AnyMessage(
                std::shared_ptr<neb::SocketChannel> pChannel,
                const MsgHead& oInMsgHead,
                const MsgBody& oInMsgBody)
{
    neb::Mydis oMemOperate;
    if (oMemOperate.ParseFromString(oInMsgBody.data()))
    {
        LOG4_TRACE("%s", oMemOperate.DebugString().c_str());
        if (!Preprocess(oMemOperate))
        {
            return(false);
        }
        if (!CheckRequest(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate))
        {
            LOG4_DEBUG("%s", oMemOperate.DebugString().c_str());
            return(false);
        }

        if (!oMemOperate.has_db_operate())
        {
            return(RedisOnly(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
        }
        if (!oMemOperate.has_redis_operate())
        {
            return(DbOnly(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
        }
        if (oMemOperate.has_db_operate() && oMemOperate.has_redis_operate())
        {
            char szRedisDataPurpose[16] = {0};
            snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
            if (neb::Mydis::RedisOperate::T_READ == oMemOperate.redis_operate().op_type())
            {
                if (m_oJsonTableRelative["relative"]("dataset") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
                {
                    if (!CheckDataSet(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate, szRedisDataPurpose))
                    {
                        return(false);
                    }
                }
                else if (m_oJsonTableRelative["relative"]("join") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
                {
                    if (!CheckJoinField(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate, szRedisDataPurpose))
                    {
                        return(false);
                    }
                }
                return(ReadEither(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
            }
            else
            {
                if (neb::Mydis::DbOperate::UPDATE == oMemOperate.db_operate().query_type())
                {
                    char szRedisDataPurpose[16] = {0};
                    snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
                    if (m_oJsonTableRelative["relative"]("dataset") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
                    {
                        return(UpdateBothWithDataset(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
                    }
                    else
                    {
                        return(WriteBoth(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
                    }
                }
                else if (neb::Mydis::DbOperate::DELETE == oMemOperate.db_operate().query_type())
                {
                    return(WriteBoth(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
                }
                else
                {
                    if (m_oJsonTableRelative["relative"]("dataset") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
                    {
                        if (!CheckDataSet(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate, szRedisDataPurpose))
                        {
                            return(false);
                        }
                        if (!PrepareForWriteBothWithDataset(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate, szRedisDataPurpose))
                        {
                            return(false);
                        }
                    }
                    else if (m_oJsonTableRelative["relative"]("join") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
                    {
                        if (!CheckJoinField(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate, szRedisDataPurpose))
                        {
                            return(false);
                        }
                        if (!PrepareForWriteBothWithFieldJoin(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate, szRedisDataPurpose))
                        {
                            return(false);
                        }
                    }
                    return(WriteBoth(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oMemOperate));
                }
            }
        }
        return(true);
    }
    else
    {
        Response(pChannel, oInMsgHead.cmd(), oInMsgHead.seq(),
                neb::ERR_PARASE_PROTOBUF, "failed to parse neb::Mydis from oInMsgBody.data()!");
        return(false);
    }
}

bool CmdMydis::Preprocess(neb::Mydis& oMemOperate)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (oMemOperate.has_redis_operate())    // 把redis命令转成全大写
    {
        int32 iKeyTtl = 0;
        char cCmdChar;
        char szRedisCmd[64] = {0};
        char szRedisDataPurpose[16] = {0};
        size_t i = 0;
        for (i = 0; i < oMemOperate.redis_operate().redis_cmd_read().size(); ++i)
        {
            cCmdChar = oMemOperate.redis_operate().redis_cmd_read()[i];
            szRedisCmd[i] = (cCmdChar <= 'z' && cCmdChar >= 'a') ? cCmdChar - ('a'-'A') : cCmdChar;
        }
        szRedisCmd[i] = '\0';
        oMemOperate.mutable_redis_operate()->set_redis_cmd_read(szRedisCmd);

        for (i = 0; i < oMemOperate.redis_operate().redis_cmd_write().size(); ++i)
        {
            cCmdChar = oMemOperate.redis_operate().redis_cmd_write()[i];
            szRedisCmd[i] = (cCmdChar <= 'z' && cCmdChar >= 'a') ? cCmdChar - ('a'-'A') : cCmdChar;
        }
        szRedisCmd[i] = '\0';
        oMemOperate.mutable_redis_operate()->set_redis_cmd_write(szRedisCmd);
        snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
        if (m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("ttl", iKeyTtl))
        {
            oMemOperate.mutable_redis_operate()->set_key_ttl(iKeyTtl);
        }

        if (m_oJsonTableRelative["section_factor_enum"]("string") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("section_factor"))
        {
            uint32 uiHash = 0;
            if (oMemOperate.redis_operate().hash_key().size() > 0)
            {
                uiHash = SessionRedisNode::hash_fnv1a_64(oMemOperate.redis_operate().hash_key().c_str(), oMemOperate.redis_operate().hash_key().size());
                oMemOperate.set_section_factor(uiHash);
            }
            else
            {
                uiHash = SessionRedisNode::hash_fnv1a_64(oMemOperate.redis_operate().key_name().c_str(), oMemOperate.redis_operate().key_name().size());
                oMemOperate.set_section_factor(uiHash);
            }
        }
    }
    return(true);
}

bool CmdMydis::CheckRequest(
                std::shared_ptr<neb::SocketChannel> pChannel,
                int32 iCmd, uint32 uiSeq,
                const neb::Mydis& oMemOperate)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (!oMemOperate.has_redis_operate() && !oMemOperate.has_db_operate())
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_INCOMPLET_DATAPROXY_DATA,
                "neighter redis_operate nor db_operate was exist!");
        return(false);
    }
    int32 iDataType;
    int32 iSectionFactorType;
    if (oMemOperate.has_redis_operate())
    {
        if (!CheckRedisOperate(pChannel, iCmd, uiSeq, oMemOperate.redis_operate()))
        {
            return(false);
        }
        char szRedisDataPurpose[16] = {0};
        snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
        if (!m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("data_type", iDataType)
                        || !m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("section_factor", iSectionFactorType))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "no \"data_type\" or \"section_factor\" config in "
                            "m_oJsonTableRelative[\"redis_struct\"][\"%s\"]", szRedisDataPurpose);
            LOG4_ERROR("error %d: %s", neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            return(false);
        }
        if (!GetNodeSession(iDataType, iSectionFactorType, oMemOperate.section_factor()))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "no route for data_type %d section_factor_type %d factor %u",
                            iDataType, iSectionFactorType, oMemOperate.section_factor());
            LOG4_ERROR("error %d: no route for data_type %d section_factor_type %d factor %u!",
                            neb::ERR_INVALID_REDIS_ROUTE, iDataType, iSectionFactorType, oMemOperate.section_factor());
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            return(false);
        }
    }
    if (oMemOperate.has_db_operate())
    {
        if (!CheckDbOperate(pChannel, iCmd, uiSeq, oMemOperate.db_operate()))
        {
            return(false);
        }
        if (!m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()].Get("data_type", iDataType)
                        || !m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()].Get("section_factor", iSectionFactorType))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "no \"data_type\" or \"section_factor\" config in "
                            "m_oJsonTableRelative[\"tables\"][\"%s\"]", oMemOperate.db_operate().table_name().c_str());
            LOG4_ERROR("error %d: %s", neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            return(false);
        }
    }
    if (oMemOperate.has_redis_operate() && oMemOperate.has_db_operate())
    {
        if (neb::Mydis::RedisOperate::T_WRITE == oMemOperate.redis_operate().op_type()
                        && neb::Mydis::DbOperate::SELECT == oMemOperate.db_operate().query_type())
        {
            Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_AND_DB_CMD_NOT_MATCH,
                            "redis_operate.op_type() and db_operate.query_type() was not match!");
            return(false);
        }
        if (neb::Mydis::RedisOperate::T_READ == oMemOperate.redis_operate().op_type()
                        && neb::Mydis::DbOperate::SELECT != oMemOperate.db_operate().query_type())
        {
            Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_AND_DB_CMD_NOT_MATCH,
                            "redis_operate.op_type() and db_operate.query_type() was not match!");
            return(false);
        }
        char szRedisDataPurpose[16] = {0};
        snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
        if (!m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("data_type", iDataType)
                        || !m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("section_factor", iSectionFactorType))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "no \"data_type\" or \"section_factor\" config in "
                            "m_oJsonTableRelative[\"redis_struct\"][\"%s\"]", szRedisDataPurpose);
            LOG4_ERROR("error %d: %s", neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            return(false);
        }
        if (!GetNodeSession(iDataType, iSectionFactorType, oMemOperate.section_factor()))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "no route for data_type %d section_factor_type %d factor %u",
                            iDataType, iSectionFactorType, oMemOperate.section_factor());
            LOG4_ERROR("error %d: no route for data_type %d section_factor_type %d factor %u!",
                            neb::ERR_INVALID_REDIS_ROUTE, iDataType, iSectionFactorType, oMemOperate.section_factor());
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
            return(false);
        }
        std::string strTableName;
        if (m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("table", strTableName))
        {
            if ((m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("data_type")
                            != m_oJsonTableRelative["tables"][strTableName]("data_type"))
                            || (m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("section_factor")
                                            != m_oJsonTableRelative["tables"][strTableName]("section_factor")))
            {
                snprintf(m_pErrBuff, gc_iErrBuffSize, "the \"data_type\" or \"section_factor\" config were "
                                "conflict in m_oJsonTableRelative[\"redis_struct\"][\"%s\"] and "
                                "m_oJsonTableRelative[\"tables\"][\"%s\"]", szRedisDataPurpose,
                                oMemOperate.db_operate().table_name().c_str());
                LOG4_ERROR("error %d: %s", neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
                Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_REDIS_ROUTE, m_pErrBuff);
                return(false);
            }
        }
    }
    return(true);
}

bool CmdMydis::CheckRedisOperate(
                std::shared_ptr<neb::SocketChannel> pChannel,
                int32 iCmd, uint32 uiSeq,
                const neb::Mydis::RedisOperate& oRedisOperate)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if ("FLUSHALL" == oRedisOperate.redis_cmd_write()
                    || "FLUSHDB" == oRedisOperate.redis_cmd_write()
                    || "FLUSHALL" == oRedisOperate.redis_cmd_read()
                    || "FLUSHDB" == oRedisOperate.redis_cmd_read())
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_CMD, "cmd flush was forbidden!");
        return(false);
    }
    neb::CJsonObject oJsonRedis;
    char szRedisDataPurpose[16] = {0};
    snprintf(szRedisDataPurpose, 16, "%d", oRedisOperate.data_purpose());
    if (!m_oJsonTableRelative["redis_struct"].Get(szRedisDataPurpose, oJsonRedis))
    {
        snprintf(m_pErrBuff, gc_iErrBuffSize, "the redis structure \"%s\" was not define!", szRedisDataPurpose);
        LOG4_ERROR("error %d: the redis structure \"%s\" was not define!",
                        neb::ERR_REDIS_STRUCTURE_NOT_DEFINE, szRedisDataPurpose);
        Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_STRUCTURE_NOT_DEFINE, m_pErrBuff);
        return(false);
    }
    /*
    if ((oJsonRedis("table").size()  > 0) && (!oMemOperate.has_db_operate()))
    {
        LOG4_ERROR("error %d: the redis structure \"%s\" was not define!",
                        neb::ERR_REDIS_STRUCTURE_NOT_DEFINE, oMemOperate.redis_operate().key_name().c_str());
        Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_STRUCTURE_NOT_DEFINE,
                        "the redis structure was not define in dataproxy config file!");
        return(false);
    }
    */
    if (oRedisOperate.redis_cmd_read().size() > 0 && oRedisOperate.redis_cmd_write().size() > 0)
    {
        if (REDIS_T_HASH == oRedisOperate.redis_structure())
        {
            if ((std::string("HVALS") == oRedisOperate.redis_cmd_read()
                            || std::string("HMGET") == oRedisOperate.redis_cmd_read()
                            || std::string("HGETALL") == oRedisOperate.redis_cmd_read())
                            && std::string("HMSET") != oRedisOperate.redis_cmd_write())
            {
                snprintf(m_pErrBuff, gc_iErrBuffSize, "the redis read cmd \"%s\" matching \"HMSET\" not \"%s\"!",
                                oRedisOperate.redis_cmd_read().c_str(), oRedisOperate.redis_cmd_write().c_str());
                LOG4_ERROR("%d: %s!", neb::ERR_REDIS_READ_WRITE_CMD_NOT_MATCH, m_pErrBuff);
                Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_READ_WRITE_CMD_NOT_MATCH, m_pErrBuff);
                return(false);
            }
            else if (std::string("HGET") == oRedisOperate.redis_cmd_read()
                            && std::string("HSET") != oRedisOperate.redis_cmd_write())
            {
                snprintf(m_pErrBuff, gc_iErrBuffSize, "the redis read cmd \"%s\" matching \"HMSET\" not \"%s\"!",
                                oRedisOperate.redis_cmd_read().c_str(), oRedisOperate.redis_cmd_write().c_str());
                LOG4_ERROR("%d: %s!", neb::ERR_REDIS_READ_WRITE_CMD_NOT_MATCH, m_pErrBuff);
                Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_READ_WRITE_CMD_NOT_MATCH, m_pErrBuff);
                return(false);
            }
        }
        else if (REDIS_T_STRING == oRedisOperate.redis_structure())
        {
            if (std::string("GET") == oRedisOperate.redis_cmd_read()
                            && std::string("SET") != oRedisOperate.redis_cmd_write())
            {
                snprintf(m_pErrBuff, gc_iErrBuffSize, "the redis read cmd \"%s\" matching \"HMSET\" not \"%s\"!",
                                oRedisOperate.redis_cmd_read().c_str(), oRedisOperate.redis_cmd_write().c_str());
                LOG4_ERROR("%d: %s!", neb::ERR_REDIS_READ_WRITE_CMD_NOT_MATCH, m_pErrBuff);
                Response(pChannel, iCmd, uiSeq, neb::ERR_REDIS_READ_WRITE_CMD_NOT_MATCH, m_pErrBuff);
                return(false);
            }
        }
    }
    return(true);
}

bool CmdMydis::CheckDbOperate(
                std::shared_ptr<neb::SocketChannel> pChannel,
                int32 iCmd, uint32 uiSeq,
                const neb::Mydis::DbOperate& oDbOperate)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (m_oJsonTableRelative["tables"][oDbOperate.table_name()]["cols"].GetArraySize() == 0)
    {
        snprintf(m_pErrBuff, gc_iErrBuffSize, "the db table \"%s\" was not define!", oDbOperate.table_name().c_str());
        LOG4_ERROR("error %d: the db table \"%s\" was not define!",
                        neb::ERR_DB_TABLE_NOT_DEFINE, oDbOperate.table_name().c_str());
        Response(pChannel, iCmd, uiSeq, neb::ERR_DB_TABLE_NOT_DEFINE, m_pErrBuff);
        return(false);
    }
    /*
    if (neb::Mydis::DbOperate::INSERT == oDbOperate.query_type()
                    || neb::Mydis::DbOperate::INSERT_IGNORE == oDbOperate.query_type()
                    || neb::Mydis::DbOperate::REPLACE == oDbOperate.query_type())
    {
        if (m_oJsonTableRelative["tables"][oDbOperate.table_name()]["cols"].GetArraySize() != oDbOperate.fields_size())
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "the db table \"%s\" field num %d, but request field num %d!",
                            oDbOperate.table_name().c_str(),
                            m_oJsonTableRelative["tables"][oDbOperate.table_name()]["cols"].GetArraySize(), oDbOperate.fields_size());
            LOG4_ERROR("error %d: the db table \"%s\" field num %d, but request field num %d!",
                            neb::ERR_LACK_TABLE_FIELD, oDbOperate.table_name().c_str(),
                            m_oJsonTableRelative["tables"][oDbOperate.table_name()]["cols"].GetArraySize(), oDbOperate.fields_size());
            Response(pChannel, iCmd, uiSeq, neb::ERR_LACK_TABLE_FIELD, m_pErrBuff);
            return(false);
        }
    }
    */
    std::map<std::string, std::set<std::string> >::iterator table_iter = m_mapTableFields.find(oDbOperate.table_name());
    if (table_iter == m_mapTableFields.end())
    {
        std::set<std::string> setTableFields;
        for (int i = 0; i < m_oJsonTableRelative["tables"][oDbOperate.table_name()]["cols"].GetArraySize(); ++i)
        {
            setTableFields.insert(m_oJsonTableRelative["tables"][oDbOperate.table_name()]["cols"](i));
        }
        m_mapTableFields.insert(std::pair<std::string, std::set<std::string> >(oDbOperate.table_name(), setTableFields));
        table_iter = m_mapTableFields.find(oDbOperate.table_name());
    }

    for (int i = 0; i < oDbOperate.fields_size(); ++i)
    {
        if (oDbOperate.fields(i).col_name().size() > 0)
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "the db table \"%s\" field name can not be empty!",
                            oDbOperate.table_name().c_str());
            LOG4_ERROR("error %d: the db table \"%s\" field name can not be empty!",
                            neb::ERR_TABLE_FIELD_NAME_EMPTY, oDbOperate.table_name().c_str());
            Response(pChannel, iCmd, uiSeq, neb::ERR_TABLE_FIELD_NAME_EMPTY, m_pErrBuff);
            return(false);
        }
        if (table_iter->second.end() == table_iter->second.find(oDbOperate.fields(i).col_name())
                        && (oDbOperate.fields(i).col_as().size() > 0
                                        && table_iter->second.end() == table_iter->second.find(oDbOperate.fields(i).col_as())))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "the query table \"%s\" field name \"%s\" is not match the table dict!",
                            oDbOperate.table_name().c_str(), oDbOperate.fields(i).col_name().c_str());
            LOG4_ERROR("error %d: the query table \"%s\" field name \"%s\" is not match the table dict!",
                            neb::ERR_DB_FIELD_ORDER_OR_FIELD_NAME, oDbOperate.table_name().c_str(), oDbOperate.fields(i).col_name().c_str());
            Response(pChannel, iCmd, uiSeq, neb::ERR_DB_FIELD_ORDER_OR_FIELD_NAME, m_pErrBuff);
            return(false);
        }
    }
    return(true);
}

bool CmdMydis::CheckDataSet(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"].GetArraySize() != oMemOperate.db_operate().fields_size())
                    // && (neb::Mydis::RedisOperate::T_WRITE == oMemOperate.redis_operate().op_type()))
    {
        snprintf(m_pErrBuff, gc_iErrBuffSize, "the query table \"%s\" field num is not match the table dict field num!",
                        oMemOperate.db_operate().table_name().c_str());
        LOG4_ERROR("error %d: the query table \"%s\" field num is not match the table dict field num!",
                        neb::ERR_DB_FIELD_NUM, oMemOperate.db_operate().table_name().c_str());
        Response(pChannel, iCmd, uiSeq, neb::ERR_DB_FIELD_NUM, m_pErrBuff);
        return(false);
    }
    bool bFoundKeyField = false;
    if (m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").size() == 0)  // 不需要key_field字段，则认为已找到
    {
        bFoundKeyField = true;
    }
    for (int i = 0; i < oMemOperate.db_operate().fields_size(); ++i)
    {
//        if (neb::Mydis::RedisOperate::T_WRITE == oMemOperate.redis_operate().op_type())
//        {
            if ((m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"](i)
                            != oMemOperate.db_operate().fields(i).col_name())
                            && (m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"](i)
                                            != oMemOperate.db_operate().fields(i).col_as()))
            {
                snprintf(m_pErrBuff, gc_iErrBuffSize, "the query table \"%s\" query field name \"%s\" "
                                "is not match the table dict field name \"%s\"!",
                                oMemOperate.db_operate().table_name().c_str(), oMemOperate.db_operate().fields(i).col_name().c_str(),
                                m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"](i).c_str());
                LOG4_ERROR("error %d: the query table \"%s\" query field name \"%s\" "
                                "is not match the table dict field name \"%s\"",
                                neb::ERR_DB_FIELD_ORDER_OR_FIELD_NAME, oMemOperate.db_operate().table_name().c_str(),
                                oMemOperate.db_operate().fields(i).col_name().c_str(),
                                m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"](i).c_str());
                Response(pChannel, iCmd, uiSeq, neb::ERR_DB_FIELD_ORDER_OR_FIELD_NAME, m_pErrBuff);
                return(false);
//            }
        }
        if (!bFoundKeyField)
        {
            if (m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"](i)
                            == m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field"))
            {
                bFoundKeyField = true;
            }
        }
    }
    if (!bFoundKeyField)
    {
        snprintf(m_pErrBuff, gc_iErrBuffSize, "key field \"%s\" not found in the table dict!",
                        m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
        LOG4_ERROR("error %d: key field \"%s\" not found in the table dict!",
                        neb::ERR_KEY_FIELD, m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
        Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, m_pErrBuff);
        return(false);
    }
    return(true);
}

bool CmdMydis::CheckJoinField(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                const neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    std::set<std::string> setReqDbFields;
    for (int i = 0; i < oMemOperate.db_operate().fields_size(); ++i)
    {
        if (oMemOperate.db_operate().fields(i).col_as().size() > 0)
        {
            setReqDbFields.insert(oMemOperate.db_operate().fields(i).col_as());
        }
        else if (oMemOperate.db_operate().fields(i).col_name().size() > 0)
        {
            setReqDbFields.insert(oMemOperate.db_operate().fields(i).col_name());
        }
    }
    if (m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").size() != 0)  // 需要检查key_field字段
    {
        if (setReqDbFields.end() == setReqDbFields.find(m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field")))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "key field \"%s\" not found in the table dict!",
                            m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
            LOG4_ERROR("error %d: key field \"%s\" not found in the table dict!",
                            neb::ERR_KEY_FIELD, m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, m_pErrBuff);
            return(false);
        }
    }
    for (int j = 0; j < m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"].GetArraySize(); ++j)
    {
        if (setReqDbFields.end() == setReqDbFields.find(m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"](j)))
        {
            snprintf(m_pErrBuff, gc_iErrBuffSize, "the query table \"%s\" join field name is not match the db request field!",
                            oMemOperate.db_operate().table_name().c_str());
            LOG4_ERROR("error %d: the query table \"%s\" join field name is not match the db request field!",
                            neb::ERR_JOIN_FIELDS, oMemOperate.db_operate().table_name().c_str());
            Response(pChannel, iCmd, uiSeq, neb::ERR_JOIN_FIELDS, m_pErrBuff);
            return(false);
        }
    }
    return(true);
}

bool CmdMydis::PrepareForWriteBothWithDataset(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    if (REDIS_T_HASH == oMemOperate.redis_operate().redis_structure())
    {
        if (m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").size() == 0)
        {
            LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "miss key_field in CmdMydisTableRelative.json config!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "miss key_field in CmdMydisTableRelative.json config!");
            return(false);
        }

        if ("HSET" == oMemOperate.redis_operate().redis_cmd_write())    // 命令如果是HSET，redis请求的field将被忽略，改用db请求中的field来填充
        {
            // 强行清空redis field并以db请求中的field来替换
            oMemOperate.mutable_redis_operate()->clear_fields();
            neb::Record oRecord;
            for (int i = 0; i < oMemOperate.db_operate().fields_size(); ++i)
            {
                if ((m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field")
                                == oMemOperate.db_operate().fields(i).col_name())
                                || (oMemOperate.db_operate().fields(i).col_as().size() > 0
                                                && m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field")
                                                == oMemOperate.db_operate().fields(i).col_as()))
                {
                    if (oMemOperate.db_operate().fields(i).col_value().size() == 0)
                    {
                        snprintf(m_pErrBuff, gc_iErrBuffSize, "the value of key field \"%s\" can not be empty!",
                                        m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                        LOG4_ERROR("error %d: the value of key field \"%s\" can not be empty!", neb::ERR_KEY_FIELD_VALUE,
                                        m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                        Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD_VALUE, m_pErrBuff);
                        return(false);
                    }
                    neb::Field* pRedisKeyField = oMemOperate.mutable_redis_operate()->add_fields();
                    pRedisKeyField->set_col_name(oMemOperate.db_operate().fields(i).col_value());
                }
                neb::Field* pDatasetField = oRecord.add_field_info();
                pDatasetField->set_col_value(oMemOperate.db_operate().fields(i).col_value());
            }
            if (oMemOperate.redis_operate().fields_size() == 0)
            {
                snprintf(m_pErrBuff, gc_iErrBuffSize, "key field \"%s\" not found in the table dict!",
                                m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                LOG4_ERROR("error %d: key field \"%s\" not found in the table dict!",
                                neb::ERR_KEY_FIELD, m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, m_pErrBuff);
                return(false);
            }
            neb::Field* pRedisField = oMemOperate.mutable_redis_operate()->mutable_fields(0);
            pRedisField->set_col_value(oRecord.SerializeAsString());
        }
        else if ("HDEL" == oMemOperate.redis_operate().redis_cmd_write())   // 命令如果是HDEL，redis后面的参数有且仅有一个，参数的名或值至少有一个不为空
        {
            if (oMemOperate.redis_operate().fields_size() != 1)
            {
                LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hdel field num must be 1!");
                Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hdel field num must be 1");
                return(false);
            }
            if (oMemOperate.redis_operate().fields(0).col_name().size() == 0)
            {
                LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hash field name is empty for hdel!");
                Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hash field name is empty for hdel!");
                return(false);
            }
            else if (oMemOperate.redis_operate().fields(0).col_value().size() > 0)
            {
                LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hash field value is not empty for hdel!");
                Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hash field value is not empty for hdel!");
                return(false);
            }
        }
        else // 命令非法
        {
            LOG4_ERROR("error %d: %s", neb::ERR_INVALID_CMD_FOR_HASH_DATASET, "hash with dataset cmd error!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_CMD_FOR_HASH_DATASET, "hash with dataset cmd error!");
            return(false);
        }
    }
    else    // REDIS_T_STRING  REDIS_T_LIST  REDIS_T_SET  and so on
    {
        if ("DEL" != oMemOperate.redis_operate().redis_cmd_write())
        {
            neb::Record oRecord;
            for (int i = 0; i < oMemOperate.db_operate().fields_size(); ++i)
            {
                neb::Field* pField = oRecord.add_field_info();
                if (oMemOperate.db_operate().fields(i).col_value().size() > 0)
                {
                    pField->set_col_value(oMemOperate.db_operate().fields(i).col_value());
                }
            }
            oMemOperate.mutable_redis_operate()->clear_fields();
            neb::Field* pRedisField = oMemOperate.mutable_redis_operate()->add_fields();
            pRedisField->set_col_value(oRecord.SerializeAsString());
        }
    }
    return(true);
}

bool CmdMydis::PrepareForWriteBothWithFieldJoin(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    std::map<std::string, std::string> mapJoinFields;
    std::map<std::string, std::string>::iterator join_field_iter;
    for (int j = 0; j < m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"].GetArraySize(); ++j)
    {
        mapJoinFields.insert(std::make_pair(m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"](j), std::string("")));
    }

    if (REDIS_T_HASH == oMemOperate.redis_operate().redis_structure() || REDIS_T_SORT_SET == oMemOperate.redis_operate().redis_structure())
    {
        if (m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").size() == 0)
        {
            LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "miss key_field in CmdMydisTableRelative.json config!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD,
                            "miss key_field in CmdMydisTableRelative.json config!");
            return(false);
        }

        if ("HSET" == oMemOperate.redis_operate().redis_cmd_write() || "ZADD" == oMemOperate.redis_operate().redis_cmd_write())    // 命令如果是HSET，redis请求的field将被忽略，改用db请求中的field来填充
        {
            if (oMemOperate.redis_operate().fields_size() == 0) // 当redis field为0时，使用db field生成redis field，否则，使用原请求的redis field
            {
                for (int i = 0; i < oMemOperate.db_operate().fields_size(); ++i)
                {
                    if ((m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field")
                                    == oMemOperate.db_operate().fields(i).col_name())
                                    || (oMemOperate.db_operate().fields(i).col_as().size() > 0
                                                    && m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field")
                                                    == oMemOperate.db_operate().fields(i).col_as()))
                    {
                        if (oMemOperate.db_operate().fields(i).col_value().size() == 0)
                        {
                            snprintf(m_pErrBuff, gc_iErrBuffSize, "the value of key field \"%s\" can not be empty!",
                                            m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                            LOG4_ERROR("error %d: the value of key field \"%s\" can not be empty!", neb::ERR_KEY_FIELD_VALUE,
                                            m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD_VALUE, m_pErrBuff);
                            return(false);
                        }
                        neb::Field* pRedisKeyField = oMemOperate.mutable_redis_operate()->add_fields();
                        pRedisKeyField->set_col_name(oMemOperate.db_operate().fields(i).col_value());
                    }
                    join_field_iter = mapJoinFields.find(oMemOperate.db_operate().fields(i).col_name());
                    if (join_field_iter == mapJoinFields.end() && oMemOperate.db_operate().fields(i).col_as().size() > 0)
                    {
                        join_field_iter = mapJoinFields.find(oMemOperate.db_operate().fields(i).col_as());
                    }
                    if (join_field_iter != mapJoinFields.end())
                    {
                        if (oMemOperate.db_operate().fields(i).col_value().size() == 0)
                        {
                            snprintf(m_pErrBuff, gc_iErrBuffSize, "the value of join field \"%s\" can not be empty!",
                                            oMemOperate.db_operate().fields(i).col_name().c_str());
                            LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD_VALUE, "the value of join field \"%s\" can not be empty!",
                                            oMemOperate.db_operate().fields(i).col_name().c_str());
                            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD_VALUE, m_pErrBuff);
                            return(false);
                        }
                        join_field_iter->second = oMemOperate.db_operate().fields(i).col_value();
                    }
                }
                if (oMemOperate.redis_operate().fields_size() == 0)
                {
                    snprintf(m_pErrBuff, gc_iErrBuffSize, "key field \"%s\" not found in the request redis operator!",
                                    m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                    LOG4_ERROR("error %d: key field \"%s\" not found in the request redis operator!",
                                    neb::ERR_LACK_JOIN_FIELDS, m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                    Response(pChannel, iCmd, uiSeq, neb::ERR_LACK_JOIN_FIELDS, m_pErrBuff);
                    return(false);
                }
                std::string strRedisFieldValue;
                for (int j = 0; j < m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"].GetArraySize(); ++j)
                {
                    join_field_iter = mapJoinFields.find(m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"](j));
                    if (strRedisFieldValue.size() == 0)
                    {
                        strRedisFieldValue = join_field_iter->second;
                    }
                    else
                    {
                        strRedisFieldValue += std::string(":") + join_field_iter->second;
                    }
                }
                neb::Field* pRedisField = oMemOperate.mutable_redis_operate()->mutable_fields(0);
                pRedisField->set_col_value(strRedisFieldValue);
            }
        }
        else if ("HDEL" == oMemOperate.redis_operate().redis_cmd_write() || "ZREM" == oMemOperate.redis_operate().redis_cmd_write())   // 命令如果是HDEL，redis后面的参数有且仅有一个，参数的名或值至少有一个不为空
        {
            if (oMemOperate.redis_operate().fields(0).col_name().size() == 0)
            {
                LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hash field name is empty for hdel!");
                Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hash field name is empty for hdel!");
                return(false);
            }
            else if (oMemOperate.redis_operate().fields(0).col_value().size() > 0)
            {
                LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hash field value is not empty for hdel!");
                Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hash field value is not empty for hdel!");
                return(false);
            }
        }
        else // 命令非法
        {
            LOG4_ERROR("error %d: %s", neb::ERR_INVALID_CMD_FOR_HASH_DATASET, "hash with field join cmd error!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_CMD_FOR_HASH_DATASET, "hash with field join cmd error!");
            return(false);
        }
    }
    else    // REDIS_T_STRING  REDIS_T_LIST  REDIS_T_SET  and so on
    {
        if ("DEL" != oMemOperate.redis_operate().redis_cmd_write())
        {
            for (int i = 0; i < oMemOperate.db_operate().fields_size(); ++i)
            {
                join_field_iter = mapJoinFields.find(oMemOperate.db_operate().fields(i).col_name());
                if (join_field_iter == mapJoinFields.end() && oMemOperate.db_operate().fields(i).col_as().size() > 0)
                {
                    join_field_iter = mapJoinFields.find(oMemOperate.db_operate().fields(i).col_as());
                }
                if (join_field_iter != mapJoinFields.end())
                {
                    if (oMemOperate.db_operate().fields(i).col_value().size() == 0)
                    {
                        snprintf(m_pErrBuff, gc_iErrBuffSize, "the value of key field \"%s\" can not be empty!",
                                        m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                        LOG4_ERROR("error %d: the value of key field \"%s\" can not be empty!",
                                        neb::ERR_KEY_FIELD_VALUE, m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]("key_field").c_str());
                        Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD_VALUE, m_pErrBuff);
                        return(false);
                    }
                    join_field_iter->second = oMemOperate.db_operate().fields(i).col_value();
                }
            }
            std::string strRedisFieldValue;
            for (int j = 0; j < m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"].GetArraySize(); ++j)
            {
                join_field_iter = mapJoinFields.find(m_oJsonTableRelative["redis_struct"][strRedisDataPurpose]["join_fields"](j));
                if (strRedisFieldValue.size() == 0)
                {
                    strRedisFieldValue = join_field_iter->second;
                }
                else
                {
                    strRedisFieldValue += std::string(":") + join_field_iter->second;
                }
            }
            oMemOperate.mutable_redis_operate()->clear_fields();
            neb::Field* pRedisField = oMemOperate.mutable_redis_operate()->add_fields();
            pRedisField->set_col_value(strRedisFieldValue);
        }
    }
    return(true);
}

bool CmdMydis::RedisOnly(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                const neb::Mydis& oMemOperate)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    if (neb::Mydis::RedisOperate::T_WRITE == oMemOperate.redis_operate().op_type())
    {
        m_pStepWriteToRedis = MakeSharedStep("mydis::StepWriteToRedis",
                pChannel, iCmd, uiSeq, oMemOperate.redis_operate(), m_pRedisNodeSession);
        if (nullptr == m_pStepWriteToRedis)
        {
            Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                            "malloc space for StepWriteToRedis error!");
            return(false);
        }
        if (neb::CMD_STATUS_RUNNING == m_pStepWriteToRedis->Emit(neb::ERR_OK))
        {
            return(true);
        }
        return(false);
    }
    else
    {
        char szRedisDataPurpose[16] = {0};
        snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
        if (m_oJsonTableRelative["relative"]("dataset") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
        {
            m_pStepReadFromRedis = MakeSharedStep("mydis::StepReadFromRedis", pChannel, iCmd, uiSeq,
                            oMemOperate.redis_operate(), m_pRedisNodeSession, true,
                            &m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"],
                            m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("key_field"));
        }
        else
        {
            m_pStepReadFromRedis = MakeSharedStep("mydis::StepReadFromRedis", pChannel, iCmd, uiSeq,
                            oMemOperate.redis_operate(), m_pRedisNodeSession, false);
        }
        if (nullptr == m_pStepReadFromRedis)
        {
            Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                            "malloc space for StepWriteToRedis error!");
            return(false);
        }
        if (neb::CMD_STATUS_RUNNING == m_pStepReadFromRedis->Emit(neb::ERR_OK))
        {
            return(true);
        }
        return(false);
    }
}

bool CmdMydis::DbOnly(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                const neb::Mydis& oMemOperate)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    m_pStepSendToDbAgent = MakeSharedStep("mydis::StepSendToDbAgent", pChannel, iCmd, uiSeq, oMemOperate, m_pRedisNodeSession);
    if (nullptr == m_pStepSendToDbAgent)
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                        "malloc space for m_pStepSendToDbAgent error!");
        return(false);
    }

    if (neb::CMD_STATUS_RUNNING == m_pStepSendToDbAgent->Emit(neb::ERR_OK))
    {
        return(true);
    }
    else
    {
        return(false);
    }
}

bool CmdMydis::ReadEither(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                const neb::Mydis& oMemOperate)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    char szRedisDataPurpose[16] = {0};
    snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
    std::string strTableName = m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("table");
    m_pStepSendToDbAgent = MakeSharedStep("mydis::StepSendToDbAgent", pChannel, iCmd, uiSeq, oMemOperate, m_pRedisNodeSession,
                    atoi(m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative").c_str()),
                    &m_oJsonTableRelative["tables"][strTableName]["cols"],
                    m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("key_field"),
                    &m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]["join_fields"]);
    if (nullptr == m_pStepSendToDbAgent)
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                        "malloc space for m_pStepSendToDbAgent error!");
        return(false);
    }

    if (m_oJsonTableRelative["relative"]("dataset") == m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("relative"))
    {
        m_pStepReadFromRedis = MakeSharedStep("mydis::StepReadFromRedis", pChannel, iCmd, uiSeq,
                        oMemOperate.redis_operate(), m_pRedisNodeSession, true,
                        &m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"],
                        m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("key_field"),
                        m_pStepSendToDbAgent);
    }
    else
    {
        m_pStepReadFromRedis = MakeSharedStep("mydis::StepReadFromRedis", pChannel, iCmd, uiSeq,
                        oMemOperate.redis_operate(), m_pRedisNodeSession, false, NULL, "", m_pStepSendToDbAgent);
    }
    if (nullptr == m_pStepReadFromRedis)
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                        "malloc space for StepWriteToRedis error!");
        return(false);
    }
    if (neb::CMD_STATUS_RUNNING == m_pStepReadFromRedis->Emit(neb::ERR_OK))
    {
        return(true);
    }
    return(false);
}

bool CmdMydis::WriteBoth(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                neb::Mydis& oMemOperate)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    m_pStepSendToDbAgent = MakeSharedStep("mydis::StepSendToDbAgent", pChannel, iCmd, uiSeq, oMemOperate, m_pRedisNodeSession);
    if (nullptr == m_pStepSendToDbAgent)
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                        "malloc space for m_pStepSendToDbAgent error!");
        return(false);
    }

    m_pStepWriteToRedis = MakeSharedStep("mydis::StepWriteToRedis", pChannel, iCmd, uiSeq, oMemOperate.redis_operate(), m_pRedisNodeSession, m_pStepSendToDbAgent);
    if (nullptr == m_pStepWriteToRedis)
    {
        Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                        "malloc space for StepWriteToRedis error!");
        return(false);
    }
    if (neb::CMD_STATUS_RUNNING == m_pStepWriteToRedis->Emit(neb::ERR_OK))
    {
        return(true);
    }
    return(false);
}

bool CmdMydis::UpdateBothWithDataset(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    neb::Mydis& oMemOperate)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    if (REDIS_T_HASH == oMemOperate.redis_operate().redis_structure())
    {
        char szRedisDataPurpose[16] = {0};
        snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
        if (m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("key_field").size() == 0)
        {
            LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "miss key_field in CmdMydisTableRelative.json config!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD,
                            "miss key_field in CmdMydisTableRelative.json config!");
            return(false);
        }
        if (("HSET" != oMemOperate.redis_operate().redis_cmd_write()
                        && "HDEL" != oMemOperate.redis_operate().redis_cmd_write())
                        || oMemOperate.redis_operate().fields_size() != 1)
        {
            LOG4_ERROR("error %d: %s", neb::ERR_INVALID_CMD_FOR_HASH_DATASET, "hash set cmd error, or invalid hash field num!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_INVALID_CMD_FOR_HASH_DATASET, "hash set cmd error, or invalid hash field num!");
            return(false);
        }
        else if (oMemOperate.redis_operate().fields(0).col_name().size() == 0)
        {
            LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hash field name is empty!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hash field name is empty!");
            return(false);
        }
        else if (oMemOperate.redis_operate().fields(0).col_value().size() > 0)
        {
            LOG4_ERROR("error %d: %s", neb::ERR_KEY_FIELD, "hash field value is not empty!");
            Response(pChannel, iCmd, uiSeq, neb::ERR_KEY_FIELD, "hash field value is not empty!");
            return(false);
        }
        oMemOperate.mutable_redis_operate()->set_redis_cmd_read("HGET");
        m_pStepReadFromRedisForWrite = MakeSharedStep("mydis::StepReadFromRedisForWrite",
                pChannel, iCmd, uiSeq, oMemOperate, m_pRedisNodeSession,
                m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"],
                m_oJsonTableRelative["redis_struct"][szRedisDataPurpose]("key_field"));
        if (nullptr == m_pStepReadFromRedisForWrite)
        {
            Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                            "malloc space for m_pStepReadFromRedisForWrite error!");
            return(false);
        }
        if (neb::CMD_STATUS_RUNNING == m_pStepReadFromRedisForWrite->Emit(neb::ERR_OK))
        {
            return(true);
        }
        return(false);
    }
    else if (REDIS_T_STRING == oMemOperate.redis_operate().redis_structure())
    {
        oMemOperate.mutable_redis_operate()->set_redis_cmd_read("GET");
        oMemOperate.mutable_redis_operate()->clear_fields();
        m_pStepReadFromRedisForWrite = MakeSharedStep("mydis::StepReadFromRedisForWrite",
                pChannel, iCmd, uiSeq, oMemOperate, m_pRedisNodeSession,
                m_oJsonTableRelative["tables"][oMemOperate.db_operate().table_name()]["cols"]);
        if (nullptr == m_pStepReadFromRedisForWrite)
        {
            Response(pChannel, iCmd, uiSeq, neb::ERR_NEW,
                            "malloc space for m_pStepReadFromRedisForWrite error!");
            return(false);
        }
        if (neb::CMD_STATUS_RUNNING == m_pStepReadFromRedisForWrite->Emit(neb::ERR_OK))
        {
            return(true);
        }
        return(false);
    }
    else
    {
        LOG4_ERROR("error %d: %s", neb::ERR_UNDEFINE_REDIS_OPERATE, "only hash an string had update dataset supported!");
        Response(pChannel, iCmd, uiSeq, neb::ERR_UNDEFINE_REDIS_OPERATE,
                        "only hash an string had update dataset supported!");
        return(false);
    }
}

void CmdMydis::Response(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                int iErrno, const std::string& strErrMsg)
{
    LOG4_TRACE("%d: %s", iErrno, strErrMsg.c_str());
    MsgBody oOutMsgBody;
    neb::Result oRsp;
    oRsp.set_err_no(iErrno);
    oRsp.set_err_msg(strErrMsg);
    oOutMsgBody.set_data(oRsp.SerializeAsString());
    SendTo(pChannel, iCmd + 1, uiSeq, oOutMsgBody);
}

bool CmdMydis::GetNodeSession(int32 iDataType, int32 iSectionFactorType, uint32 uiFactor)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szFactor[64] = {0};
    snprintf(szFactor, 64, "%d:%d", iDataType, iSectionFactorType);
    std::map<std::string, std::set<uint32> >::const_iterator c_factor_iter =  m_mapFactorSection.find(szFactor);
    if (c_factor_iter == m_mapFactorSection.end())
    {
        return(false);
    }
    else
    {
        std::set<uint32>::const_iterator c_section_iter = c_factor_iter->second.lower_bound(uiFactor);
        if (c_section_iter == c_factor_iter->second.end())
        {
            return(false);
        }
        else
        {
            snprintf(szFactor, 64, "SessionRedisNode-%u:%u:%u", iDataType, iSectionFactorType, *c_section_iter);
            m_pRedisNodeSession = std::dynamic_pointer_cast<SessionRedisNode>(GetSession(std::string(szFactor)));
            return(true);
        }
    }
}

} /* namespace mydis */
