/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepDbDistribute.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef SRC_CMDLOCATEDATA_STEPDBDISTRIBUTE_HPP_
#define SRC_CMDLOCATEDATA_STEPDBDISTRIBUTE_HPP_

#include "util/json/CJsonObject.hpp"
#include "pb/msg.pb.h"
#include "DbStorageStep.hpp"

namespace mydis
{

class StepDbDistribute: public DbStorageStep,
    public neb::DynamicCreator<StepDbDistribute, neb::CJsonObject*>
{
public:
    StepDbDistribute(const neb::CJsonObject* pRedisNode = NULL);
    virtual ~StepDbDistribute();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    std::shared_ptr<neb::SocketChannel> pChannel,
                    const MsgHead& oInMsgHead,
                    const MsgBody& oInMsgBody,
                    void* data = NULL);

    virtual neb::E_CMD_STATUS Timeout();

private:
    neb::CJsonObject m_oRedisNode;
};

} /* namespace mydis */

#endif /* SRC_CMDLOCATEDATA_STEPDBDISTRIBUTE_HPP_ */
