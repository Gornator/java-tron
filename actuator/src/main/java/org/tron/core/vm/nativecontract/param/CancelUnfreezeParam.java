package org.tron.core.vm.nativecontract.param;

public class CancelUnfreezeParam {

  private byte[] ownerAddress;

  public byte[] getOwnerAddress() {
    return ownerAddress;
  }

  public void setOwnerAddress(byte[] ownerAddress) {
    this.ownerAddress = ownerAddress;
  }
}
