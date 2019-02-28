/*******************************************************************************
 * Project:  NebulaMydis
 * @file     CmdMydis.hpp
 * @brief    数据访问代理
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_CMDDATAPROXY_HPP_
#define MYDIS_CMDDATAPROXY_HPP_

#include <dirent.h>
#include <map>
#include <set>
#include "actor/cmd/Cmd.hpp"
#include "mydis/MydisOperator.hpp"
#include "pb/mydis.pb.h"
#include "SessionRedisNode.hpp"
//#include "SessionSyncDbData.hpp"
#include "StepReadFromRedis.hpp"
#include "StepSendToDbAgent.hpp"
#include "StepWriteToRedis.hpp"
#include "StepReadFromRedisForWrite.hpp"
//#include "StepSyncToDb.hpp"

namespace mydis
{

const int gc_iErrBuffSize = 256;

class CmdMydis: public neb::Cmd, public neb::DynamicCreator<CmdMydis, int32>
{
public:
    CmdMydis(int32 iCmd);
    virtual ~CmdMydis();

    virtual bool Init();

    virtual bool AnyMessage(
                    std::shared_ptr<neb::SocketChannel> pChannel, 
                    const MsgHead& oMsgHead,
                    const MsgBody& oMsgBody);

protected:
    bool ReadDataProxyConf();
    bool ReadTableRelation();
    bool ScanSyncData();

    bool Preprocess(neb::Mydis& oMemOperate);
    bool CheckRequest(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate);
    bool CheckRedisOperate(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis::RedisOperate& oRedisOperate);
    bool CheckDbOperate(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis::DbOperate& oDbOperate);

    bool CheckDataSet(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose);
    bool CheckJoinField(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose);
    bool PrepareForWriteBothWithDataset(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose);
    bool PrepareForWriteBothWithFieldJoin(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    neb::Mydis& oMemOperate, const std::string& strRedisDataPurpose);

    bool RedisOnly(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate);
    bool DbOnly(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate);
    bool ReadEither(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    const neb::Mydis& oMemOperate);
    bool WriteBoth(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    neb::Mydis& oMemOperate);
    bool UpdateBothWithDataset(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    neb::Mydis& oMemOperate);

    void Response(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq,
                    int iErrno, const std::string& strErrMsg);

private:
    bool GetNodeSession(int32 iDataType, int32 iSectionFactorType, uint32 uiFactor);

private:
    char* m_pErrBuff;
    std::shared_ptr<SessionRedisNode> m_pRedisNodeSession;
    neb::CJsonObject m_oJsonTableRelative;
    std::map<std::string, std::set<uint32> > m_mapFactorSection; //分段因子区间配置，key为因子类型
    std::map<std::string, std::set<std::string> > m_mapTableFields; //表的组成字段，key为表名，value为字段名集合，用于查找请求的字段名是否存在

    std::shared_ptr<neb::Step> m_pStepSendToDbAgent;
    std::shared_ptr<neb::Step> m_pStepReadFromRedis;
    std::shared_ptr<neb::Step> m_pStepWriteToRedis;
    std::shared_ptr<neb::Step> m_pStepReadFromRedisForWrite;
    std::shared_ptr<neb::Step> m_pStepSyncToDb;
};

} /* namespace mydis */

#endif /* MYDIS_CMDDATAPROXY_HPP_ */
