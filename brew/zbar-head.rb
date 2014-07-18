require 'formula'

class ZbarHead < Formula
  homepage 'http://zbar.sourceforge.net'
  head 'http://hg.code.sf.net/p/zbar/code', :using => :hg

  depends_on :autoconf
  depends_on :automake
  depends_on 'libtool'
  depends_on 'libiconv'
  depends_on 'pkg-config'
  depends_on 'xmlto'
  depends_on :x11 => :optional
  depends_on 'pkg-config' => :build
  depends_on 'jpeg'
  depends_on 'imagemagick'
  depends_on 'ufraw'

  def install
    # Remove -Werror
    inreplace 'configure.ac', 'AM_INIT_AUTOMAKE([1.10 -Wall -Werror foreign subdir-objects std-options dist-bzip2])', 'AM_INIT_AUTOMAKE([1.10 -Wall foreign subdir-objects std-options dist-bzip2])'

    system "autoreconf", "-ivf"

    args = %W[
      --disable-dependency-tracking
      --prefix=#{prefix}
      --without-python
      --without-qt
      --disable-video
      --without-gtk
    ]

    if build.with? 'x11'
      args << '--with-x'
    else
      args << '--without-x'
    end

    system "./configure", *args
    system "make install"
  end
end
