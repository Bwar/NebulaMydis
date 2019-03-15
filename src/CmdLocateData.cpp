/*******************************************************************************
 * Project:  NebulaMydis
 * @file     CmdLocateData.cpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#include "CmdLocateData.hpp"

namespace mydis
{

CmdLocateData::CmdLocateData(int32 iCmd)
    : neb::Cmd(iCmd),
      m_pRedisNodeSession(nullptr), m_pCurrentContext(nullptr), m_pStepDbDistribute(nullptr)
{
}

CmdLocateData::~CmdLocateData()
{
}

bool CmdLocateData::Init()
{
    if (ReadDataProxyConf() && ReadTableRelation())
    {
        return(true);
    }
    return(false);
}

bool CmdLocateData::ReadDataProxyConf()
{
    neb::CJsonObject oConfJson;
    //配置文件路径查找
    std::string strConfFile = GetWorkPath() + std::string("/conf/mydis/CmdMydis.json");

    std::ifstream fin(strConfFile.c_str());

    if (fin.good())
    {
        std::stringstream ssContent;
        ssContent << fin.rdbuf();
        fin.close();
        if (oConfJson.Parse(ssContent.str()))
        {
            //LOG4_DEBUG("oConfJson pasre OK");
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
                                                    pNodeSession = std::dynamic_pointer_cast<SessionRedisNode>(MakeSharedSession("mydis::SessionRedisNode",
                                                            std::string(szSessionId), iHashAlgorithm, iVirtualNodeNum, 0.0));
                                                }
                                                strRedisNode = oConfJson["cluster"][oConfJson["data_type"](i)][oConfJson["section_factor"](j)][szFactorSectionKey](l);
                                                if (oConfJson["redis_group"][strRedisNode].Get("master", strMaster)
                                                                && oConfJson["redis_group"][strRedisNode].Get("slave", strSlave))
                                                {
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

bool CmdLocateData::ReadTableRelation()
{
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

bool CmdLocateData::AnyMessage(
                std::shared_ptr<neb::SocketChannel> pChannel,
                const MsgHead& oInMsgHead,
                const MsgBody& oInMsgBody)
{
    neb::Mydis oMemOperate;
    if (oMemOperate.ParseFromString(oInMsgBody.data()))
    {
        std::stringstream ssSessionId;
        ssSessionId << "mydis::ContextRequest:" << GetSequence();
        m_pCurrentContext = std::dynamic_pointer_cast<neb::Context>(MakeSharedSession(
                "mydis::ContextRequest", ssSessionId.str(), pChannel, oInMsgHead.cmd(), oInMsgHead.seq(), oInMsgBody));
        if (nullptr == m_pCurrentContext)
        {
            return(false);
        }

        if (!oMemOperate.has_db_operate())
        {
            return(RedisOnly(oMemOperate));
        }
        if (!oMemOperate.has_redis_operate())
        {
            return(DbOnly());
        }
        if (oMemOperate.has_db_operate() && oMemOperate.has_redis_operate())
        {
            return(RedisAndDb());
        }
        m_pCurrentContext->Response(neb::ERR_INCOMPLET_DATAPROXY_DATA, "neighter redis_operate nor db_operate was exist!");
        return(false);
    }
    else
    {
        m_pCurrentContext->Response(neb::ERR_PARASE_PROTOBUF, "failed to parse neb::Mydis from oInMsgBody.data()!");
        return(false);
    }
}

bool CmdLocateData::RedisOnly(const neb::Mydis& oMemOperate)
{
    char szRedisDataPurpose[16] = {0};
    char szFactor[32] = {0};
    char szErrMsg[128] = {0};
    int32 iDataType = 0;
    int32 iSectionFactorType = 0;
    snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
    m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("data_type", iDataType);
    m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("section_factor", iSectionFactorType);
    snprintf(szFactor, 32, "%d:%d", iDataType, iSectionFactorType);
    std::map<std::string, std::set<uint32> >::const_iterator c_factor_iter =  m_mapFactorSection.find(szFactor);
    if (c_factor_iter == m_mapFactorSection.end())
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "no redis node session for data_type %u and section_factor_type %u!",
                        iDataType, iSectionFactorType);
        m_pCurrentContext->Response(neb::ERR_LACK_CLUSTER_INFO, szErrMsg);
        return(false);
    }
    else
    {
        std::set<uint32>::const_iterator c_section_iter = c_factor_iter->second.lower_bound(oMemOperate.section_factor());
        if (c_section_iter == c_factor_iter->second.end())
        {
            snprintf(szErrMsg, sizeof(szErrMsg), "no redis node found for data_type %u and section_factor_type %u and factor %u!",
                            iDataType, iSectionFactorType, oMemOperate.section_factor());
            m_pCurrentContext->Response(neb::ERR_LACK_CLUSTER_INFO, szErrMsg);
            return(false);
        }
        else
        {
            std::string strMasterIdentify;
            std::string strSlaveIdentify;
            snprintf(szFactor, 32, "SessionRedisNode-%u:%u:%u", iDataType, iSectionFactorType, *c_section_iter);
            m_pRedisNodeSession = std::dynamic_pointer_cast<SessionRedisNode>(GetSession(std::string(szFactor)));
            if (oMemOperate.redis_operate().hash_key().size() > 0)
            {
                m_pRedisNodeSession->GetRedisNode(oMemOperate.redis_operate().hash_key(), strMasterIdentify, strSlaveIdentify);
            }
            else
            {
                m_pRedisNodeSession->GetRedisNode(oMemOperate.redis_operate().key_name(), strMasterIdentify, strSlaveIdentify);
            }
            MsgBody oOutMsgBody;
            neb::CJsonObject oRspJson;
            oRspJson.Add("code", neb::ERR_OK);
            oRspJson.Add("msg", "successfully");
            oRspJson.Add("redis_node", neb::CJsonObject("{}"));
            oRspJson["redis_node"].Add("master", strMasterIdentify);
            oRspJson["redis_node"].Add("slave", strSlaveIdentify);
            oOutMsgBody.set_data(oRspJson.ToFormattedString());
            SendTo(m_pCurrentContext->GetChannel(), m_pCurrentContext->GetCmd() + 1, m_pCurrentContext->GetSeq(), oOutMsgBody);
            return(true);
        }
    }
}

bool CmdLocateData::DbOnly()
{
    neb::CJsonObject oRedisNode;
    m_pStepDbDistribute = MakeSharedStep("mydis::StepDbDistribute", &oRedisNode);
    if (nullptr == m_pStepDbDistribute)
    {
        m_pCurrentContext->Response(neb::ERR_NEW, "malloc space for m_pStepDbDistribute error!");
        return(false);
    }

    if (neb::CMD_STATUS_RUNNING == m_pStepDbDistribute->Emit(neb::ERR_OK))
    {
        return(true);
    }
    else
    {
        return(false);
    }
}

bool CmdLocateData::RedisAndDb()
{
    neb::Mydis oMemOperate;
    oMemOperate.ParseFromString(GetContext()->GetMsgBody().data());
    char szRedisDataPurpose[16] = {0};
    char szFactor[32] = {0};
    char szErrMsg[128] = {0};
    int32 iDataType = 0;
    int32 iSectionFactorType = 0;
    snprintf(szRedisDataPurpose, 16, "%d", oMemOperate.redis_operate().data_purpose());
    m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("data_type", iDataType);
    m_oJsonTableRelative["redis_struct"][szRedisDataPurpose].Get("section_factor", iSectionFactorType);
    snprintf(szFactor, 32, "%d:%d", iDataType, iSectionFactorType);
    std::map<std::string, std::set<uint32> >::const_iterator c_factor_iter =  m_mapFactorSection.find(szFactor);
    if (c_factor_iter == m_mapFactorSection.end())
    {
        snprintf(szErrMsg, sizeof(szErrMsg), "no redis node session for data_type %u and section_factor_type %u!",
                        iDataType, iSectionFactorType);
        m_pCurrentContext->Response(neb::ERR_LACK_CLUSTER_INFO, szErrMsg);
        return(false);
    }
    else
    {
        std::set<uint32>::const_iterator c_section_iter = c_factor_iter->second.lower_bound(oMemOperate.section_factor());
        if (c_section_iter == c_factor_iter->second.end())
        {
            snprintf(szErrMsg, sizeof(szErrMsg), "no redis node found for data_type %u and section_factor_type %u and factor %u!",
                            iDataType, iSectionFactorType, oMemOperate.section_factor());
            m_pCurrentContext->Response(neb::ERR_LACK_CLUSTER_INFO, szErrMsg);
            return(false);
        }
        else
        {
            std::string strMasterIdentify;
            std::string strSlaveIdentify;
            snprintf(szFactor, 32, "SessionRedisNode-%u:%u:%u", iDataType, iSectionFactorType, *c_section_iter);
            m_pRedisNodeSession = std::dynamic_pointer_cast<SessionRedisNode>(GetSession(std::string(szFactor)));
            if (oMemOperate.redis_operate().hash_key().size() > 0)
            {
                m_pRedisNodeSession->GetRedisNode(oMemOperate.redis_operate().hash_key(), strMasterIdentify, strSlaveIdentify);
            }
            else
            {
                m_pRedisNodeSession->GetRedisNode(oMemOperate.redis_operate().key_name(), strMasterIdentify, strSlaveIdentify);
            }
            neb::CJsonObject oRedisNodeJson;
            oRedisNodeJson.Add("master", strMasterIdentify);
            oRedisNodeJson.Add("slave", strSlaveIdentify);
            m_pStepDbDistribute = MakeSharedStep("mydis::StepDbDistribute", &oRedisNodeJson);
            if (nullptr == m_pStepDbDistribute)
            {
                m_pCurrentContext->Response(neb::ERR_NEW, "malloc space for m_pStepDbDistribute error!");
                return(false);
            }

            if (neb::CMD_STATUS_RUNNING == m_pStepDbDistribute->Emit(neb::ERR_OK))
            {
                return(true);
            }
            else
            {
                return(false);
            }
        }
    }
}

} /* namespace mydis */
