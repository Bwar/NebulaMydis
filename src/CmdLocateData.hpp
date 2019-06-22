/*******************************************************************************
 * Project:  NebulaMydis
 * @file     CmdLocateData.hpp
 * @brief    查询数据落在集群的哪个节点
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef SRC_CMDLOCATEDATA_CMDLOCATEDATA_HPP_
#define SRC_CMDLOCATEDATA_CMDLOCATEDATA_HPP_

#include <fstream>
#include "actor/cmd/Cmd.hpp"
#include "actor/context/PbContext.hpp"
#include "pb/mydis.pb.h"
#include "SessionRedisNode.hpp"
#include "StepDbDistribute.hpp"

namespace mydis
{

/**
 * @brief 查询数据落在集群的哪个节点
 * @note
 */
class CmdLocateData: public neb::Cmd, public neb::DynamicCreator<CmdLocateData, int32>
{
public:
    CmdLocateData(int32 iCmd);
    virtual ~CmdLocateData();

    virtual bool Init();

    virtual bool AnyMessage(
                    std::shared_ptr<neb::SocketChannel> pChannel,
                    const MsgHead& oInMsgHead,
                    const MsgBody& oInMsgBody);
protected:
    bool ReadDataProxyConf();
    bool ReadTableRelation();
    bool RedisOnly(const neb::Mydis& oMemOperate);
    bool DbOnly();
    bool RedisAndDb();

private:
    std::shared_ptr<SessionRedisNode> m_pRedisNodeSession;
    neb::CJsonObject m_oJsonTableRelative;
    std::map<std::string, std::set<uint32> > m_mapFactorSection; //分段因子区间配置，key为因子类型

    std::shared_ptr<neb::PbContext> m_pCurrentContext;
    std::shared_ptr<neb::Step> m_pStepDbDistribute;
};

} /* namespace mydis */

#endif /* SRC_CMDLOCATEDATA_CMDLOCATEDATA_HPP_ */
